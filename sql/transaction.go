package sql

import (
	"context"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"time"
)

// 支持Spring Transactional的事务传播机制

type TransactionPropagation int8

const (
	PropagationRequired     = iota // 如果存在一个事务，则支持当前事务，如果当前没有事务，就新建一个事务
	PropagationSupports            // 如果存在一个事务，支持当前事务，如果当前没有事务，就以非事务方式执行
	PropagationMandatory           // 如果存在一个事务，支持当前事务，如果当前没有事务，返回错误
	PropagationRequiresNew         // 新建事务，如果当前存在事务，把当前事务挂起
	PropagationNotSupported        // 以非事务方式执行操作，如果当前存在事务，就把当前事务挂起
	PropagationNested              // 支持当前事务，新增Savepoint点，与当前事务同步提交或回滚
	PropagationNever               // 以非事务方式执行，如果当前存在事务，直接返回错误
)

func defaultPropagation() TransactionPropagation {
	return PropagationRequired
}

var (
	ErrCommitWithoutTransaction        = errors.New("not in transaction, can't commit")
	ErrNeverPropInTransaction          = errors.New("never propagation must not in transaction")
	ErrMandatoryPropWithoutTransaction = errors.New("mandatory propagation must in transaction")
)

type transactionContext struct {
	ctx    context.Context
	tx     *gorm.DB
	parent *transactionContext
}

func (c *transactionContext) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *transactionContext) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *transactionContext) Err() error {
	return c.ctx.Err()
}

func (c *transactionContext) Value(key interface{}) interface{} {
	return c.ctx.Value(key)
}

func (c *transactionContext) IsRoot() bool {
	return c.parent == nil
}

func (c *transactionContext) Ctx() context.Context {
	return c.ctx
}

func (c *transactionContext) TxDB() *gorm.DB {
	return c.tx
}

func (c *transactionContext) TxError() error {
	if c.tx != nil {
		return c.tx.Error
	}
	return nil
}

func (c *transactionContext) InTransaction() bool {
	if c.tx == nil {
		return false
	}
	committer, ok := c.tx.Statement.ConnPool.(gorm.TxCommitter)
	return ok && committer != nil
}

func (c *transactionContext) Session() *transactionContext {
	return &transactionContext{
		ctx:    c.ctx,
		tx:     c.tx.WithContext(c.ctx),
		parent: c,
	}
}

func (c *transactionContext) Rollback() {
	if c.InTransaction() && c.IsRoot() {
		c.tx.Rollback()
	}
}

func (c *transactionContext) Commit() error {
	if !c.InTransaction() {
		return ErrCommitWithoutTransaction
	}
	if c.IsRoot() {
		return c.tx.Commit().Error
	}
	return nil
}

type TransactionManager interface {
	DBFactory
	Transaction(ctx context.Context, bizFn func(ctx context.Context, tx *gorm.DB) error, propagations ...TransactionPropagation) error
}

type transactionManager struct {
	dBFactory DBFactory
}

func NewTransactionManager(factory DBFactory) TransactionManager {
	return &transactionManager{
		dBFactory: factory,
	}
}

func (m *transactionManager) GetDB(ctx context.Context) *gorm.DB {
	if txCtx, ok := ctx.(*transactionContext); ok && txCtx.tx != nil {
		return txCtx.tx
	}
	return m.getPureDB(ctx)
}

func (m *transactionManager) GetOriginDB() *gorm.DB {
	return m.dBFactory.GetOriginDB()
}

func (m *transactionManager) getPureDB(ctx context.Context) *gorm.DB {
	return m.dBFactory.GetDB(ctx)
}

func (m *transactionManager) Transaction(ctx context.Context, bizFn func(ctx context.Context, tx *gorm.DB) error, propagations ...TransactionPropagation) error {
	propagation := defaultPropagation()
	if len(propagations) > 0 {
		propagation = propagations[0]
	}
	switch propagation {
	case PropagationRequired:
		return m.withRequiredPropagation(ctx, bizFn)
	case PropagationSupports:
		return m.withSupportsPropagation(ctx, bizFn)
	case PropagationMandatory:
		return m.withMandatoryPropagation(ctx, bizFn)
	case PropagationRequiresNew:
		return m.withRequiresNewPropagation(ctx, bizFn)
	case PropagationNotSupported:
		return m.withNotSupportedPropagation(ctx, bizFn)
	case PropagationNested:
		return m.withNestedPropagation(ctx, bizFn)
	case PropagationNever:
		return m.withNeverPropagation(ctx, bizFn)
	default:
		panic("not supported propagation")
	}
}

func (m *transactionManager) withNeverPropagation(ctx context.Context, bizFn func(ctx context.Context, tx *gorm.DB) error) error {
	if txCtx, ok := ctx.(*transactionContext); ok && txCtx.InTransaction() {
		return ErrNeverPropInTransaction
	}

	db := m.getPureDB(ctx)
	return bizFn(ctx, db)
}

func (m *transactionManager) withNestedPropagation(ctx context.Context, bizFn func(ctx context.Context, tx *gorm.DB) error) error {
	var err error
	if txCtx, ok := ctx.(*transactionContext); ok && txCtx.InTransaction() {
		panicked := true
		db := txCtx.TxDB()
		if !db.DisableNestedTransaction {
			err = db.SavePoint(fmt.Sprintf("sp%p", bizFn)).Error
			defer func() {
				// Make sure to rollback when panic, Block error or Commit error
				if panicked || err != nil {
					db.RollbackTo(fmt.Sprintf("sp%p", bizFn))
				}
			}()
		}
		if err == nil {
			err = bizFn(txCtx.Session(), txCtx.TxDB())
		}
		panicked = false
	} else {
		err = m.withRequiredPropagation(ctx, bizFn)
	}
	return err
}

func (m *transactionManager) withRequiredPropagation(ctx context.Context, bizFn func(ctx context.Context, tx *gorm.DB) error) error {
	var err error
	panicked := true
	if txCtx, ok := ctx.(*transactionContext); ok && txCtx.InTransaction() {
		// There is no need to handle errors and panics here, the outer transaction manager will handle it
		err = bizFn(txCtx.Session(), txCtx.tx)
	} else {
		var db *gorm.DB
		db = m.getPureDB(ctx)
		if !ok {
			txCtx = &transactionContext{
				ctx: ctx,
				tx:  db.Begin(),
			}
		} else {
			txCtx.tx = db.Begin()
		}
		defer func() {
			if panicked || err != nil {
				txCtx.Rollback()
			}
		}()
		if err = txCtx.TxError(); err == nil {
			err = bizFn(txCtx, txCtx.tx)
		}

		if err == nil {
			err = txCtx.Commit()
		}
	}
	panicked = false
	return err
}

func (m *transactionManager) withRequiresNewPropagation(ctx context.Context, bizFn func(ctx context.Context, tx *gorm.DB) error) error {
	panicked := true
	var pureCtx context.Context
	if txCtx, ok := ctx.(*transactionContext); ok {
		pureCtx = txCtx.Ctx()
	} else {
		pureCtx = ctx
	}
	var err error
	db := m.getPureDB(pureCtx)

	txCtx := &transactionContext{
		ctx: ctx,
		tx:  db.Begin(),
	}
	defer func() {
		if panicked || err != nil {
			txCtx.Rollback()
		}
	}()
	if err = txCtx.TxError(); err == nil {
		err = bizFn(txCtx, txCtx.tx)
	}

	if err == nil {
		err = txCtx.Commit()
	}
	panicked = false
	return err
}

func (m *transactionManager) withSupportsPropagation(ctx context.Context, bizFn func(ctx context.Context, tx *gorm.DB) error) error {
	if txCtx, ok := ctx.(*transactionContext); ok && txCtx.InTransaction() {
		// There is no need to handle errors and panics because the outer transaction manager will handle it
		return bizFn(txCtx.Session(), txCtx.tx)
	} else {
		db := m.getPureDB(ctx)
		return bizFn(ctx, db)
	}
}

func (m *transactionManager) withMandatoryPropagation(ctx context.Context, bizFn func(ctx context.Context, tx *gorm.DB) error) error {
	if txCtx, ok := ctx.(*transactionContext); ok && txCtx.InTransaction() {
		// There is no need to handle errors and panics because the outer transaction manager will handle it
		return bizFn(txCtx.Session(), txCtx.tx)
	} else {
		return ErrMandatoryPropWithoutTransaction
	}
}

func (m *transactionManager) withNotSupportedPropagation(ctx context.Context, bizFn func(ctx context.Context, tx *gorm.DB) error) error {
	var pureCtx context.Context
	if txCtx, ok := ctx.(*transactionContext); ok {
		pureCtx = txCtx.Ctx()
	} else {
		pureCtx = ctx
	}
	db := m.getPureDB(pureCtx)
	return bizFn(pureCtx, db)
}

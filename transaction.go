package propagation_tx

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"time"
)

// 支持类似java Spring Transactional的事务传播机制

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
	Transaction(ctx context.Context, bizFn func(ctx context.Context, tx *gorm.DB) error, propagations ...TransactionPropagation) error
}

type transactionManager struct {
}

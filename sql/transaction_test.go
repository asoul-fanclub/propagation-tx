package sql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

var (
	db, _      = GetSimpleDB("localhost", 3306, "fanchat", "root", "140214mysql", context.Background())
	factory, _ = NewSimpleDBFactory("localhost", 3306, "fanchat", "root", "140214mysql")
	tm         = NewTransactionManager(factory)
	mockErr    = errors.New("mock error")
	mockPanic  = func() { panic("mock panic") }
)

var (
	user1 = &User{Username: "test_user_1", CreateTime: time.Now()}
	user2 = &User{Username: "test_user_2", CreateTime: time.Now()}
	user3 = &User{Username: "test_user_3", CreateTime: time.Now()}
	user4 = &User{Username: "test_user_4", CreateTime: time.Now()}
)

type User struct {
	Id         int32     `gorm:"column:id;type:int;not null;primaryKey;autoIncrement"`
	Username   string    `gorm:"column:username;type:varchar(255);not null"`
	CreateTime time.Time `gorm:"column:create_time;type:datetime;not null"`
}

func (user *User) TableName() string {
	return "user"
}

func TestSimpleDB(t *testing.T) {
	clearData()
	_ = db.Create(user1)

	var user *User
	_ = db.Find(&user, "username = ?", user1.Username)
	assert.Equal(t, user1.Username, user.Username)
	_ = db.Delete(User{}, "username = ?", user1.Username)
	user = nil
	assert.Nil(t, user)
	clearData()
}

func TestTransactionManager_Transaction_PropagationRequired(t *testing.T) {
	DefaultTransactionTest("test-all-commit", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return nil
			}, PropagationRequired); err != nil {
				return err
			}

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)
				return nil
			}, PropagationRequired); err != nil {
				return err
			}
			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertExist(t, user1)
		AssertExist(t, user2)
		AssertExist(t, user3)
	})

	DefaultTransactionTest("test-user1-panic-rollback", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				mockPanic()
				return nil
			}, PropagationRequired); err != nil {
				return err
			}

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)
				return nil
			}, PropagationRequired); err != nil {
				return err
			}
			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertNotExist(t, user1)
		AssertNotExist(t, user2)
		AssertNotExist(t, user3)
	})

	DefaultTransactionTest("test-user4-error-rollback", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return mockErr
			}, PropagationRequired); err != nil {
				return err
			}

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)

				if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
					tx.Create(user4)
					return mockErr
				}, PropagationRequired); err != nil {
					return err
				}

				return nil
			}, PropagationRequired); err != nil {
				return err
			}
			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertNotExist(t, user1)
		AssertNotExist(t, user2)
		AssertNotExist(t, user3)
		AssertNotExist(t, user4)
	})

	DefaultTransactionTest("test-user4-panic-rollback", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return nil
			}, PropagationRequired); err != nil {
				return err
			}

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)

				if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
					tx.Create(user4)
					mockPanic()
					return nil
				}, PropagationRequired); err != nil {
					return err
				}

				return nil
			}, PropagationRequired); err != nil {
				return mockErr
			}
			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertNotExist(t, user1)
		AssertNotExist(t, user2)
		AssertNotExist(t, user3)
		AssertNotExist(t, user4)
	})

	DefaultTransactionTest("test-user3-error-ignore-not-rollback", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return nil
			}, PropagationRequired); err != nil {
				return err
			}

			// error ignore, not rollback
			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)
				return mockErr
			}, PropagationRequired)

			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertExist(t, user1)
		AssertExist(t, user2)
		AssertExist(t, user3)
	})

	DefaultTransactionTest("test-user3-panic-ignore-not-rollback", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return nil
			}, PropagationRequired); err != nil {
				return err
			}

			// panic ignore, not rollback
			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)
				mockPanic()
				return mockErr
			}, PropagationRequired)

			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertExist(t, user1)
		AssertExist(t, user2)
		AssertExist(t, user3)
	})

	DefaultTransactionTest("test-user4-error-ignore-not-rollback", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return nil
			}, PropagationRequired); err != nil {
				return err
			}

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)

				// ignore error, not rollback
				_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
					tx.Create(user4)
					return mockErr
				}, PropagationRequired)

				return nil
			}, PropagationRequired); err != nil {
				return err
			}

			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertExist(t, user1)
		AssertExist(t, user2)
		AssertExist(t, user3)
		AssertExist(t, user4)
	})

	DefaultTransactionTest("test-user4-panic-ignore-not-rollback", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return nil
			}, PropagationRequired); err != nil {
				return err
			}

			if err := tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)

				// ignore panic, not rollback
				_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
					tx.Create(user4)
					mockPanic()
					return nil
				}, PropagationRequired)

				return nil
			}, PropagationRequired); err != nil {
				return err
			}

			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertExist(t, user1)
		AssertExist(t, user2)
		AssertExist(t, user3)
		AssertExist(t, user4)
	})

}

func TestTransactionManager_Transaction_PropagationRequiresNew(t *testing.T) {
	DefaultTransactionTest("test-user1-rollback", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx,
			func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user1)

				_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
					tx.Create(user2)
					return nil
				}, PropagationRequiresNew)

				_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
					tx.Create(user3)
					return nil
				}, PropagationRequiresNew)

				return mockErr
			}, PropagationRequiresNew)
	}, func(t *testing.T) {
		AssertNotExist(t, user1)
		AssertExist(t, user2)
		AssertExist(t, user3)
	})
}

func DefaultTransactionTest(name string, t *testing.T, testFn func(), checkFn func(t *testing.T)) {
	TransactionTest(name, t, func() { clearData() }, func() { clearData() }, testFn, checkFn)
}

func TransactionTest(name string, t *testing.T, initFn func(), cleanFn func(), testFn func(), checkFn func(t *testing.T)) {
	t.Run(name, func(t *testing.T) {
		initFn()
		defer cleanFn()
		defer func() {
			if r := recover(); r != nil {
				t.Logf("test: [%v] catch panic: %v", name, r)
			}
		}()
		testFn()
		checkFn(t)
	})
}

func AssertNotExist(t *testing.T, user *User) {
	var count int64 = 0
	db.Model(&User{}).Where("username = ?", user.Username).Count(&count)
	assert.Equal(t, int64(0), count)
}

func AssertExist(t *testing.T, user *User) {
	var count int64 = 0
	db.Model(&User{}).Where("username = ?", user.Username).Count(&count)
	assert.Equal(t, int64(1), count)
}

func clearData() {
	db.Delete(User{}, "1=1")
}

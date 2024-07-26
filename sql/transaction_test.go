package sql

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"testing"
)

var (
	db, _                              = GetSimpleDB("localhost", 3306, "pt", "root", "123456", context.Background())
	factory, _                         = NewSimpleDBFactory("localhost", 3306, "pt", "root", "123456")
	tm                                 = NewTransactionManager(factory)
	mockErr                            = errors.New("mock error")
	mockPanic                          = func() { panic("mock panic") }
	errMandatoryPropWithoutTransaction = errors.New("mandatory propagation must in transaction")
	errNeverPropInTransaction          = errors.New("never propagation must not in transaction")
)

var (
	user1 = &User{Username: "test_user_1"}
	user2 = &User{Username: "test_user_2"}
	user3 = &User{Username: "test_user_3"}
	user4 = &User{Username: "test_user_4"}
)

type User struct {
	Id       int32  `gorm:"column:id;type:int;not null;primaryKey;autoIncrement" json:"id,omitempty"`
	Username string `gorm:"column:username;type:varchar(255);not null" json:"username,omitempty"`
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
	_ = db.Find(&user, "username = ?", user1.Username)
	assert.Equal(t, "", user.Username)
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

	DefaultTransactionTest("test-outside-error-all-rollback", t, func() {
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
			return mockErr
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertNotExist(t, user1)
		AssertNotExist(t, user2)
		AssertNotExist(t, user3)
	})

	DefaultTransactionTest("test-inside-error-all-rollback", t, func() {
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

	DefaultTransactionTest("test-outside-panic-all-rollback", t, func() {
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
			mockPanic()
			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertNotExist(t, user1)
		AssertNotExist(t, user2)
		AssertNotExist(t, user3)
	})

	DefaultTransactionTest("test-inside-panic-all-rollback", t, func() {
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
				mockPanic()
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
}

func TestTransactionManager_Transaction_PropagationSupports(t *testing.T) {
	DefaultTransactionTest("test-outside-tx-inside-tx-rollback-all", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			var err error
			tx.Create(user1)
			err = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return mockErr
			}, PropagationSupports)
			return err
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertNotExist(t, user1)
		AssertNotExist(t, user2)
	})

	DefaultTransactionTest("test-outside-not-tx-inside-not-tx-all-commit", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			var err error
			tx.Create(user1)
			err = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return mockErr
			}, PropagationSupports)
			return err
		}, PropagationNever)
	}, func(t *testing.T) {
		AssertExist(t, user1)
		AssertExist(t, user2)
	})
}

func TestTransactionManager__Transaction_PropagationMandatory(t *testing.T) {
	var err error
	DefaultTransactionTest("test-tx-not-error", t, func() {
		ctx := context.Background()
		err = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)
			return tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return nil
			}, PropagationMandatory)
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertExist(t, user1)
		AssertExist(t, user2)
		assert.Nil(t, err)
	})

	err = nil
	DefaultTransactionTest("test-not-tx-error", t, func() {
		ctx := context.Background()
		err = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)
			return nil
		}, PropagationMandatory)
	}, func(t *testing.T) {
		AssertNotExist(t, user1)
		AssertErrorsIsEqual(t, err, errMandatoryPropWithoutTransaction)
	})
}

func TestTransactionManager_Transaction_PropagationRequiresNew(t *testing.T) {
	DefaultTransactionTest("test-inside-error-outside-commit-inside-rollback", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return mockErr
			}, PropagationRequiresNew)

			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertExist(t, user1)
		AssertNotExist(t, user2)
	})

	DefaultTransactionTest("test-outside-error-inside-commit-outside-rollback", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return nil
			}, PropagationRequiresNew)

			return mockErr
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertNotExist(t, user1)
		AssertExist(t, user2)
	})
}

func TestTransactionManager_Transaction_PropagationNotSupported(t *testing.T) {
	DefaultTransactionTest("test-create-success", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return mockErr
			}, PropagationNotSupported)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)
				return nil
			}, PropagationNotSupported)

			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertExist(t, user1)
		AssertExist(t, user2)
		AssertExist(t, user3)
	})

	DefaultTransactionTest("test-create-success", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return nil
			}, PropagationNotSupported)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)
				return nil
			}, PropagationRequired)

			return mockErr
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertNotExist(t, user1)
		AssertExist(t, user2)
		AssertNotExist(t, user3)
	})
}

func TestTransactionManager_Transaction_PropagationNested(t *testing.T) {
	DefaultTransactionTest("test-all-rollback", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return nil
			}, PropagationNested)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)
				return nil
			}, PropagationNested)

			AssertNotExist(t, user1)
			AssertNotExist(t, user2)
			AssertNotExist(t, user3)
			return mockErr
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertNotExist(t, user1)
		AssertNotExist(t, user2)
		AssertNotExist(t, user3)
	})

	DefaultTransactionTest("test-commit-together", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return nil
			}, PropagationNested)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)
				return nil
			}, PropagationNested)

			AssertNotExist(t, user1)
			AssertNotExist(t, user2)
			AssertNotExist(t, user3)
			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertExist(t, user1)
		AssertExist(t, user2)
		AssertExist(t, user3)
	})

	DefaultTransactionTest("test-commit-together", t, func() {
		ctx := context.Background()
		_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return mockErr
			}, PropagationNested)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)
				return nil
			}, PropagationNested)

			AssertNotExist(t, user1)
			AssertNotExist(t, user2)
			AssertNotExist(t, user3)
			return nil
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertExist(t, user1)
		AssertNotExist(t, user2)
		AssertExist(t, user3)
	})
}

func TestTransactionManager_Transaction_PropagationNever(t *testing.T) {
	var err error
	DefaultTransactionTest("test-not-transaction-success", t, func() {
		ctx := context.Background()
		err = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)
			return nil
		}, PropagationNever)
	}, func(t *testing.T) {
		AssertExist(t, user1)
		assert.Nil(t, err)
	})

	err = nil
	DefaultTransactionTest("test-transaction-error", t, func() {
		ctx := context.Background()
		err = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)
			return tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				// directly return error and not execute the following code
				tx.Create(user2)
				return nil
			}, PropagationNever)
		}, PropagationRequired)
	}, func(t *testing.T) {
		AssertNotExist(t, user1)
		AssertNotExist(t, user2)
		AssertErrorsIsEqual(t, err, errNeverPropInTransaction)
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
				checkFn(t)
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

func AssertErrorsIsEqual(t *testing.T, err1, err2 error) {
	if err1.Error() != err2.Error() {
		t.Errorf("errors: [%v], [%v] should equal", err1, err2)
	}
}

func clearData() {
	db.Delete(User{}, "1=1")
}

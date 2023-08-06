package main

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"propagation-tx/sql"
	"propagation-tx/test"
)

var db, _ = sql.GetSimpleDB("localhost", 3306, "fanchat", "root", "140214mysql", context.Background())

func TestSimpleDB(t *testing.T) {
	clearData()
	_ = db.Create(&test.User{
		Username: "test_username",
		Password: "test_password",
		Salt:     "test_salt",
	})

	var user *test.User
	_ = db.Find(&user, "username = ?", "test_username")
	assert.Equal(t, "test_username", user.Username)
	assert.Equal(t, "test_password", user.Password)
	assert.Equal(t, "test_salt", user.Salt)
	_ = db.Delete(test.User{}, "username = ?", "test_username")
	user = nil
	assert.Nil(t, user)
	clearData()
}

func TestTranslate(t *testing.T) {
	clearData()
	fa, err := sql.NewSimpleDBFactory("localhost", 3306, "fanchat", "root", "140214mysql")
	assert.Nil(t, err)
	ctx := context.Background()
	user1 := &test.User{Username: "test_user_1"}
	user2 := &test.User{Username: "test_user_2"}
	user3 := &test.User{Username: "test_user_3"}
	tm := sql.NewTransactionManager(fa)
	mockErr := errors.New("mock error")

	_ = tm.Transaction(ctx,
		func(ctx context.Context, tx *gorm.DB) error {
			tx.Create(user1)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user2)
				return nil
			}, sql.PropagationRequiresNew)

			_ = tm.Transaction(ctx, func(ctx context.Context, tx *gorm.DB) error {
				tx.Create(user3)
				return nil
			}, sql.PropagationRequiresNew)

			return mockErr
		}, sql.PropagationRequiresNew)

	AssertNotExist(t, user1)
	AssertExist(t, user2)
	AssertExist(t, user3)
	clearData()
}

func AssertNotExist(t *testing.T, user *test.User) {
	var count int64 = 0
	db.Model(&test.User{}).Where("username = ?", user.Username).Count(&count)
	assert.Equal(t, int64(0), count)
}

func AssertExist(t *testing.T, user *test.User) {
	var count int64 = 0
	db.Model(&test.User{}).Where("username = ?", user.Username).Count(&count)
	assert.Equal(t, int64(1), count)
}

func clearData() {
	db.Delete(test.User{}, "1=1")
}

package main

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"propagation-tx/sql"
	"propagation-tx/test"
	"testing"
)

func TestSimpleDB(t *testing.T) {
	db, err := sql.GetSimpleDB("localhost", 3306, "fanchat", "root", "140214mysql", context.Background())
	if err != nil {
		return
	}
	db.Create(&test.User{
		Username: "test_username",
		Password: "test_password",
		Salt:     "test_salt",
	})
	var user *test.User
	db.Find(&user, "username = ?", "test_username")
	assert.Equal(t, "test_username", user.Username)
	assert.Equal(t, "test_password", user.Password)
	assert.Equal(t, "test_salt", user.Salt)
	db.Delete(test.User{}, "username = ?", "test_username")
	user = nil
	assert.Nil(t, user)
}

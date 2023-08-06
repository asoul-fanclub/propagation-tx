package test

type User struct {
	Id       int32  `gorm:"column:id;type:int;not null;primaryKey;autoIncrement"`
	Username string `gorm:"column:username;type:varchar(255);not null"`
	Password string `gorm:"column:password;type:varchar(255);not null"`
	Salt     string `gorm:"column:salt;type:varchar(255);not null"`
}

func (user *User) TableName() string {
	return "user"
}

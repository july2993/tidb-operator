package util

import (
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	. "github.com/onsi/gomega"
	"github.com/pingcap/tidb-operator/cmd/backup-manager/app/constants"
)

func TestGenericOptions(t *testing.T) {
	g := NewGomegaWithT(t)

	bo := &GenericOptions{
		Namespace:    "ns",
		ResourceName: "rn",
		User:         "root",
		Password:     "123456",
		Host:         "localhost",
		Port:         3306,
	}

	// test String()
	g.Expect(bo.String()).Should(Equal("ns/rn"))

	// test GetDSN
	// TODO test enabledTLSClient
	dsn := fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=utf8", bo.User, bo.Password, bo.Host, bo.Port, constants.TidbMetaDB)
	getDSN, err := bo.GetDSN(false)
	g.Expect(err).Should(BeNil())
	g.Expect(getDSN).Should(Equal(dsn))

	// test GetTikvGCLifeTime
	db, mock, err := sqlmock.New()
	g.Expect(err).Should(BeNil())
	gcValue := "2h"
	mock.ExpectQuery("select variable_value from").WillReturnRows(sqlmock.NewRows([]string{"gg"}).AddRow(gcValue))
	getTime, err := bo.GetTikvGCLifeTime(db)
	g.Expect(err).Should(BeNil())
	g.Expect(getTime).Should(Equal(gcValue))

	// test SetTikvGCLifeTime
	db, mock, err = sqlmock.New()
	g.Expect(err).Should(BeNil())
	mock.ExpectExec(fmt.Sprintf("update %s set variable_value = \\? where variable_name = \\?", constants.TidbMetaTable)).
		WithArgs(gcValue, constants.TikvGCVariable).
		WillReturnResult(sqlmock.NewResult(0, 1))
	err = bo.SetTikvGCLifeTime(db, gcValue)
	g.Expect(err).Should(BeNil())
}

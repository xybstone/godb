package xdb

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bernos/go-retry"

	"github.com/go-xorm/xorm"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

const (
	MySQL      = "mysql"
	Postgresql = "postgres"
)

var (
	MaxIdleConns = 50
	MaxOpenConns = 200
)

var mLock sync.Mutex
var logger DbLogger

//SQLDriver driver interface
type SQLDriver interface {
	GetDatabase() string
	GetUser() string
	GetPwd() string
	GetHost() string
	GetPort() string
	GetDriver() string
}

var sqlDrivers map[string]SQLDriver
var sqlEngines map[string]*xorm.Engine

func init() {
	sqlDrivers = make(map[string]SQLDriver)
	sqlEngines = make(map[string]*xorm.Engine)
}

var errDB = errors.New("db name error")
var errDriver = errors.New("only support mysql and postgres")

func AddSQLDriver(key string, sd SQLDriver) {
	sqlDrivers[key] = sd
}

func newEngine(db string) func() (interface{}, error) {
	return func() (interface{}, error) {
		var eng *xorm.Engine
		var err error

		if d, has := sqlDrivers[db]; has {
			driver := d.GetDriver()
			var driverDSN string
			switch driver {
			case MySQL:
				driverDSN = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", d.GetUser(), d.GetPwd(), d.GetHost(), d.GetPort(), d.GetDatabase())
				break
			case Postgresql:
				driverDSN = fmt.Sprintf("user=%s password=%s port=%s dbname=%s host=%s sslmode=disable", d.GetUser(), d.GetPwd(), d.GetPort(), d.GetDatabase(), d.GetHost())
				break
			default:
				return nil, errDriver
			}

			eng, err = xorm.NewEngine(driver, driverDSN)
			if err == nil {
				sqlLogger := xorm.NewSimpleLogger(logger)
				sqlLogger.ShowSQL(true)
				eng.SetLogger(sqlLogger)
				eng.ShowExecTime(true)
				eng.SetMaxIdleConns(MaxIdle)
				eng.SetMaxOpenConns(MaxOpenConns)
			}
		} else {
			return nil, errDB
		}
		return eng, err
	}
}

//getEngine 获取engine
func getEngine(db string) (*xorm.Engine, error) {
	r := retry.Retry(
		newEngine(db),
		retry.MaxRetries(5),
		retry.BaseDelay(time.Millisecond*200),
		retry.Log(logger.Logger),
	)

	e, err := r()
	if err != nil {
		return nil, err
	}

	return e.(*xorm.Engine), nil
}

//GetEngine 获取数据库
func GetEngine(db string) (*xorm.Engine, error) {
	mLock.Lock()
	defer mLock.Unlock()
	var err error
	eng, has := sqlEngines[db]
	if !has || eng == nil {
		eng, err = getEngine(db)
		sqlEngines[db] = eng
	} else if err := eng.Ping(); err != nil {
		eng.Close()
		eng, err = getEngine(db)
		sqlEngines[db] = eng
	}
	return eng, err
}

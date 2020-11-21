import pymysql
from db.database import database


class HandleDatabase():
    DATABASE_CONNECT = "mysql"
    cursor = None
    db = None

    def __init__(self):
        self.db = self.connectDatabase()
        self.cursor = self.db.cursor()

    def connectDatabase(self):
        conf = database[self.DATABASE_CONNECT]
        return pymysql.connect(
            conf["DB_HOST"], conf["DB_USERNAME"], conf["DB_PASSWORD"], conf["DB_DATABASE"])

    def createDatabase(self, dbname):
        sql = "create database if not exists "+dbname
        self.cursor.execute(sql)
        self.db.close()

    def createTable(self, sql):
        self.cursor.execute(sql)
        self.db.close()

    def truncateTable(self, tableName):
        sql = "truncate table %s" % (tableName)
        self.cursor.execute(sql)
        self.db.close()

    def create(self, sql):
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except pymysql.DatabaseError:
            self.db.rollback()
            self.db.close()

    def update(self, sql):
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except pymysql.DatabaseError:
            self.db.rollback()
            self.db.close()

    def delete(self, sql):
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except pymysql.DatabaseError:
            self.db.rollback()
            self.db.close()

    def firstOrCreate(self, fetch_sql, sql):
        try:
            self.cursor.execute(fetch_sql)
            data = self.cursor.fetchone()
            if(data is None):
                self.cursor.execute(sql)
                self.db.commit()
        except pymysql.DatabaseError:
            self.db.rollback()
            self.db.close()

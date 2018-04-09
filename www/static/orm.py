# -*- coding:utf-8 -*-


__author__ = "lsy"

import logging; logging.basicConfig(level=logging.INFO)
import aiomysql


async def create_pool(loop, **kw):
    logging.info("create database connection pool....")
    global __pool
    __pool = await aiomysql.create_pool(
        host = kw.get("host", "localhost"),
        port = kw.get("port", 3306),
        user = kw.get("user"),
        password = kw.get("password"),
        db = kw.get("db"),
        charset = kw.get("charset", "utf8"),
        autocommit = kw.get("autocommit", True),
        maxsize = kw.get("maxsize", 10),
        minsize = kw.get("minsize", 1),
        loop = loop
    )
async def destroy_pool():
    global __pool
    if __pool is not None:
        __pool.close()
        await __pool.wait_closed()
async def select(sql, args,size = None):
    logging.info(sql, args)
    async with __pool.acquire() as conn:
        #指定conn.cursor(aiomysql.DictCursor)的返回值为字典(存在于元组中)
        # 如({'age': 0L, 'num': 1000L}, {'age': 0L, 'num': 2000L}, {'age': 0L, 'num': 3000L})
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', "%s"), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
            logging.info("rows returned %d" % len(rs))
            logging.info("These result are ", rs)
            return rs

async def execute(sql, args):
    # logging.info(sql, args)
    async with __pool.acquire() as conn:
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                #此处execute的参数为元组； cursor.execute("SELECT * FROM t1 WHERE id = %s", (5,))
                await cur.execute(sql.replace('?', "%s"), args)
                affected = cur.rowcount
        except BaseException:
            raise
        return affected

def create_args_string(n):
    l = []
    for i in range(n):
        l.append('?')
    return ','.join(l)
class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    def __str__(self):
        return "<%s,%s:%s>" % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    def __init__(self, name = None, column_type = "varchar(100)", \
                 primary_key = False, default = None):
        super().__init__(name, column_type, primary_key, default)

class IntegerField(Field):
    def __init__(self, name = None, primary_key = False, default = 0):
        super().__init__(name, "bigint", primary_key, default)
class FloatField(Field):
    def __init__(self, name = None, primary_key = False, default = 0.0):
        super().__init__(name, "real", primary_key, default)
class BooleanField(Field):
    def __init__(self, name = None, default = False):
        super().__init__(name, "boolean", False, default)
class TextField(Field):
    def __init__(self, name = None, default = None):
        super().__init__(name, "text", False, default)
class ModelMetaclass(type):
    def __new__(cls, name, bases, attr):
        #排除Model类本身
        if name == "Model":
            return type.__new__(cls, name, bases, attr)
        #获取表名
        tableName = attr.get("__table__", None) or name
        logging.info("found moudel %s table:%s" % (name, tableName))
        #获取所有的Field和主键名
        fields = []#储存mapping中所有的键(属性名)，除去主键
        mapping = dict()#属性名和列的映射(列有列名，数据类型，是否为主键，默认值)
        primaryKey = None
        for k, v in attr.items():
            if isinstance(v, Field):
                mapping[k] = v
                logging.info("found mapping %s =======> %s" % (k, v))
                if v.primary_key:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)#表示只能有1个主键
                    else:
                        primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            logging.info("primary key not found")
        #完成属性到列的映射，删除类中的属性，防止通过实例取到类属性
        for k in mapping.keys():
            attr.pop(k)
        escaped_fields = list(map(lambda f: "`%s`" % f, fields))
        attr["__mapping__"] = mapping
        attr["__fields__"] = fields
        attr["__table__"] = tableName
        attr["__primary_key__"] = primaryKey
        attr["__select__"] = "select `%s`,%s from `%s`" % (primaryKey, ','.join(escaped_fields), tableName)
        attr["__insert__"] = "insert into `%s` (%s, `%s`) values (%s)" %(tableName, ','.join(escaped_fields), primaryKey,\
                                                                      create_args_string(len(escaped_fields)+1))
        attr["__update__"] = "update `%s` set %s where `%s`=?" % (tableName, ','.join(escaped_fields),primaryKey)
        attr["__delete__"] = "delete from `%s` where `%s`= ?" % (tableName,primaryKey)
        return type.__new__(cls, name, bases, attr)
class Model(dict, metaclass = ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mapping__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.info('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value
    @classmethod
    async def find(cls, pk):
        rs = await select("%s where `%s`= ?" % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        else:
            #这里查询一个，获取元组中的第一项就是结果字典，在通过关键字参数生成对应类的实例
            return cls(**rs[0])
    @classmethod
    async def findAll(cls):
        rs = await select("%s" % cls.__select__)
        if len(rs) == 0:
            return None
        else:
            rl = []
            for r in rs:
                rl.append(cls(**r))
            return rl

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        print("self.__fields__------------>", self.__fields__)
        print("args------------>", args)
        rows = await execute("%s" % self.__insert__, args)
        if rows != 1:
            logging.warning("failed to insert record: affect rows: %d" % rows)



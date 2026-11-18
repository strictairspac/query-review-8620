from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,DeclarativeBase
from sqlalchemy.pool import NullPool
from .settings import DATABASE_URL
engine=create_engine(DATABASE_URL,pool_pre_ping=True)
# 回退名额使用会话级 advisory lock；NullPool 保证 connection.close() 真正断开、锁随会话释放
lock_engine=create_engine(DATABASE_URL,poolclass=NullPool)
Session=sessionmaker(engine,expire_on_commit=False)
class Base(DeclarativeBase): pass
def get_db():
    with Session() as db: yield db

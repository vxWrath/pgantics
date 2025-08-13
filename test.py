import datetime

from pgantics import Column, Mapped, Table, types


class User(Table):
    class Meta:
        table_name = "users"

    id: Mapped[int] = Column(types.BigSerial(), primary_key=True)
    email: Mapped[str] = Column(types.VarChar(100))
    first_name: Mapped[str] = Column(types.VarChar(50))
    last_name: Mapped[str] = Column(types.VarChar(50))
    age: Mapped[int] = Column(types.Integer())
    created_at: Mapped[datetime.datetime] = Column(types.TimestampTZ())
    salary: Mapped[float] = Column(types.Real())

class Post(Table):
    class Meta:
        table_name = "posts"

    id: Mapped[int] = Column(types.BigSerial(), primary_key=True)
    user_id: Mapped[int] = Column(types.BigInt())
    title: Mapped[str] = Column(types.VarChar(200))
    content: Mapped[str] = Column(types.Text())
    views: Mapped[int] = Column(types.Integer())
    created_at: Mapped[datetime.datetime] = Column(types.TimestampTZ())

print("=== Original Query ===")
query1 = (User
    .select(User.id, Post.content)
    .join(Post).on(Post.user_id == User.id)
    .where((Post.created_at > User.created_at) & (User.created_at > datetime.datetime.now()))
    .where(Post.content != "test")
    .order_by(User.created_at.desc())
    .limit(10)
    .offset(5)
    .build()
)
print(query1[0])
print("Parameters:", query1[1])
print()
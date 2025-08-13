import datetime

from pgantics import Column, Mapped, Table, types


class User(Table):
    class Meta:
        table_name = "users"

    id: Mapped[int] = Column(types.BigSerial(), primary_key=True)
    email: Mapped[str] = Column(types.VarChar(100))
    created_at: Mapped[datetime.datetime] = Column(types.TimestampTZ())

class Post(Table):
    class Meta:
        table_name = "posts"

    id: Mapped[int] = Column(types.BigSerial(), primary_key=True)
    user_id: Mapped[int] = Column(types.BigInt())
    content: Mapped[str] = Column(types.Text())
    created_at: Mapped[datetime.datetime] = Column(types.TimestampTZ())

print(User
    .select(User.id, Post.content)
    .join(Post).on(Post.user_id == User.id)
    .where((Post.created_at > User.created_at) & (User.created_at > datetime.datetime.now()))
    .where(Post.content != "test")
    .order_by(User.created_at.desc())
    .limit(10)
    .offset(5)
    .build()
)
import datetime

from pgantics import Column, Mapped, Table, format_build, funcs, types


# Define test tables
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

class Profile(Table):
    class Meta:
        table_name = "profiles"
    
    id: Mapped[int] = Column(types.BigSerial(), primary_key=True)
    user_id: Mapped[int] = Column(types.BigInt())
    bio: Mapped[str] = Column(types.Text())
    avatar_url: Mapped[str] = Column(types.VarChar(255))

# Basic query
print(User.select(User.id, User.email).where(User.age > 18).build())
print()

# Complex join with conditions
print(User.select(User.email, Post.title)
 .join(Post).on(Post.user_id == User.id)
 .where((User.age > 21) & (Post.views > 100))
 .order_by(User.created_at.desc())
 .limit(10).build())
print()
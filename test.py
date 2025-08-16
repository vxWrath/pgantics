import datetime

from pgantics import Column, Mapped, Table, format_query, types


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

user = User(
    id = 123,
    email="user@example.com",
    first_name="John",
    last_name="Doe",
    age=30,
    created_at=datetime.datetime.now(),
    salary=50000.0
)

query = Post.delete().join(User).on(Post.user_id == User.id).join(Profile).on(Profile.user_id == User.id).where(User.age < 18).where(Profile.bio.is_null())

print("Complex DELETE with multiple JOINs:")
print(format_query(query))
print()

# More examples:
print("Simple DELETE:")
simple = User.delete().where(User.age < 18)
print(format_query(simple))
print()

print("DELETE with single JOIN:")
single_join = Post.delete().join(User).on(Post.user_id == User.id).where(User.age < 18)
print(format_query(single_join))
print()

print("DELETE with RETURNING:")
with_returning = User.delete().where(User.email.like('%@spam.com')).returning('id', 'email')
print(format_query(with_returning))
print()
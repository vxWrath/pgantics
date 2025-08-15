import datetime

from pgantics import Column, Mapped, Table, format_build, types


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

b = user.insert().on_conflict(User.email).do_update({User.first_name: "Jane"}).build()
print(format_build(*b))
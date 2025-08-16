import datetime

from pgantics import Column, Mapped, Table, format_query, funcs, types


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
    updated_at: Mapped[datetime.datetime] = Column(types.TimestampTZ())
    salary: Mapped[float] = Column(types.Real())
    active: Mapped[bool] = Column(types.Boolean())

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
    verified: Mapped[bool] = Column(types.Boolean())

user = User(
    id=123,
    email="newemail@example.com",
    first_name="John",
    last_name="Doe",
    age=31,
    created_at=datetime.datetime.now(),
    updated_at=datetime.datetime.now(),
    salary=55000.0,
    active=True
)

print("1. Simple UPDATE (all non-primary key columns):")
simple_update = user.update().where(User.id == 123)
print(format_query(simple_update))
print()

print("2. UPDATE specific columns only:")
specific_update = user.update('email', 'age').where(User.id == 123)
print(format_query(specific_update))
print()

print("3. UPDATE with manual SET values:")
manual_update = user.update('email').override({
    'updated_at': funcs.Now(),
    'age': User.age + 1
}).where(User.id == 123)
print(format_query(manual_update))
print()

print("4. UPDATE with JOIN:")
join_update = user.update().join(Profile).on(Profile.user_id == User.id).where(Profile.verified == True)
print(format_query(join_update))
print()

print("5. UPDATE with RETURNING:")
returning_update = user.update().where(User.id == 123).returning('id', 'email', 'updated_at')
print(format_query(returning_update))
print()
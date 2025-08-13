import datetime

from pgantics import Column, Mapped, Table, format_build, funcs, types
from pgantics.enums import JoinType


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

def test_basic_queries():
    """Test basic SELECT queries."""
    print("=== Basic SELECT Queries ===")
    
    # Simple select all
    query1 = User.select()
    sql1, params1 = query1.build()
    print("1. Select all users:")
    print(f"   SQL: {sql1}")
    print(f"   Params: {params1}")
    print()
    
    # Select specific columns
    query2 = User.select(User.id, User.email, User.first_name)
    sql2, params2 = query2.build()
    print("2. Select specific columns:")
    print(f"   SQL: {sql2}")
    print(f"   Params: {params2}")
    print()
    
    # Select with string column names
    query3 = User.select('id', 'email', 'first_name')
    sql3, params3 = query3.build()
    print("3. Select with string column names:")
    print(f"   SQL: {sql3}")
    print(f"   Params: {params3}")
    print()

def test_where_conditions():
    """Test WHERE clauses with various conditions."""
    print("=== WHERE Conditions ===")
    
    # Simple equality
    query1 = User.select().where(User.age == 25)
    sql1, params1 = query1.build()
    print("1. Simple equality:")
    print(f"   SQL: {sql1}")
    print(f"   Params: {params1}")
    print()
    
    # Multiple conditions with AND
    query2 = User.select().where(User.age > 18).where(User.salary >= 50000)
    sql2, params2 = query2.build()
    print("2. Multiple AND conditions:")
    print(f"   SQL: {sql2}")
    print(f"   Params: {params2}")
    print()
    
    # Complex boolean logic
    query3 = User.select().where((User.age > 25) & (User.salary > 60000) | (User.email.like('%@admin.com')))
    sql3, params3 = query3.build()
    print("3. Complex boolean logic:")
    print(f"   SQL: {sql3}")
    print(f"   Params: {params3}")
    print()
    
    # IN clause
    query4 = User.select().where(User.id.in_([1, 2, 3, 4, 5]))
    sql4, params4 = query4.build()
    print("4. IN clause:")
    print(f"   SQL: {sql4}")
    print(f"   Params: {params4}")
    print()
    
    # BETWEEN clause
    query5 = User.select().where(User.age.between(25, 35))
    sql5, params5 = query5.build()
    print("5. BETWEEN clause:")
    print(f"   SQL: {sql5}")
    print(f"   Params: {params5}")
    print()

def test_joins():
    """Test JOIN operations."""
    print("=== JOIN Operations ===")
    
    # Inner join
    query1 = (User.select(User.id, User.email, Post.title)
             .join(Post).on(Post.user_id == User.id))
    sql1, params1 = query1.build()
    print("1. Inner join:")
    print(f"   SQL: {sql1}")
    print(f"   Params: {params1}")
    print()
    
    # Left join
    query2 = (User.select(User.id, User.email, Profile.bio)
             .join(Profile, JoinType.LEFT).on(Profile.user_id == User.id))
    sql2, params2 = query2.build()
    print("2. Left join:")
    print(f"   SQL: {sql2}")
    print(f"   Params: {params2}")
    print()
    
    # Multiple joins
    query3 = (User.select(User.email, Post.title, Profile.bio)
             .join(Post).on(Post.user_id == User.id)
             .join(Profile, JoinType.LEFT).on(Profile.user_id == User.id))
    sql3, params3 = query3.build()
    print("3. Multiple joins:")
    print(f"   SQL: {sql3}")
    print(f"   Params: {params3}")
    print()

def test_aggregation():
    """Test aggregation functions and GROUP BY."""
    print("=== Aggregation and GROUP BY ===")
    
    # Count all users
    query1 = User.select(funcs.Count())
    sql1, params1 = query1.build()
    print("1. Count all users:")
    print(f"   SQL: {sql1}")
    print(f"   Params: {params1}")
    print()
    
    # Group by with aggregation
    query2 = (User.select('age', funcs.Count().as_alias('user_count'))
             .group_by('age'))
    sql2, params2 = query2.build()
    print("2. Group by age with count:")
    print(f"   SQL: {sql2}")
    print(f"   Params: {params2}")
    print()
    
    # Having clause
    query3 = (User.select('age', funcs.Count().as_alias('user_count'))
             .group_by('age')
             .having(funcs.Count() > 5))
    sql3, params3 = query3.build()
    print("3. Group by with HAVING:")
    print(f"   SQL: {sql3}")
    print(f"   Params: {params3}")
    print()

def test_ordering_and_limiting():
    """Test ORDER BY, LIMIT, and OFFSET."""
    print("=== Ordering and Limiting ===")
    
    # Order by single column
    query1 = User.select().order_by(User.created_at.desc())
    sql1, params1 = query1.build()
    print("1. Order by created_at DESC:")
    print(f"   SQL: {sql1}")
    print(f"   Params: {params1}")
    print()
    
    # Multiple order expressions
    query2 = User.select().order_by(User.age.asc(), User.salary.desc())
    sql2, params2 = query2.build()
    print("2. Multiple order expressions:")
    print(f"   SQL: {sql2}")
    print(f"   Params: {params2}")
    print()
    
    # With limit and offset
    query3 = (User.select()
             .order_by(User.created_at.desc())
             .limit(10)
             .offset(20))
    sql3, params3 = query3.build()
    print("3. With LIMIT and OFFSET:")
    print(f"   SQL: {sql3}")
    print(f"   Params: {params3}")
    print()

def test_complex_query():
    """Test a complex query combining multiple features."""
    print("=== Complex Query Example ===")
    
    query = (User.select(User.id, User.email, Post.title, funcs.Count(Post.id).as_alias('post_count'))
            .join(Post).on(Post.user_id == User.id)
            .join(Profile, JoinType.LEFT).on(Profile.user_id == User.id)
            .where((User.age > 21) & (Post.views > 100))
            .where(User.created_at > datetime.datetime(2023, 1, 1))
            .group_by(User.id, User.email, Post.title)
            .having(funcs.Count(Post.id) >= 2)
            .order_by(User.created_at.desc(), funcs.Count(Post.id).desc())
            .limit(25)
            .offset(10))
    
    sql, params = query.build()
    print("Complex query with joins, conditions, grouping, and ordering:")
    print(f"SQL: {sql}")
    print(f"Params: {params}")
    print()

def test_distinct():
    """Test DISTINCT queries."""
    print("=== DISTINCT Queries ===")
    
    query1 = User.select('age').distinct()
    sql1, params1 = query1.build()
    print("1. Select distinct ages:")
    print(f"   {format_build(sql1, params1)}")
    print()

def test_functions_and_expressions():
    """Test function calls and expressions in SELECT."""
    print("=== Functions and Expressions ===")
    
    # String functions
    query1 = User.select(funcs.Upper(User.first_name).as_alias('upper_name'), 
                        funcs.Length(User.email).as_alias('email_length'))
    sql1, params1 = query1.build()
    print("1. String functions:")
    print(f"   {format_build(sql1, params1)}")
    print()
    
    # Arithmetic expressions
    query2 = User.select(User.id, (User.salary * 12).as_alias('yearly_salary'))
    sql2, params2 = query2.build()
    print("2. Arithmetic expressions:")
    print(f"   {format_build(sql2, params2)}")
    print()
    
    # CASE expressions
    query3 = User.select(User.id, 
                        funcs.Case()
                        .when(User.age < 25, 'Young')
                        .when(User.age < 50, 'Middle-aged')
                        .else_('Senior')
                        .as_alias('age_group'))
    sql3, params3 = query3.build()
    print("3. CASE expression:")
    print(f"   {format_build(sql3, params3)}")
    print()

if __name__ == "__main__":
    test_basic_queries()
    test_where_conditions()
    test_joins()
    test_aggregation()
    test_ordering_and_limiting()
    test_complex_query()
    test_distinct()
    test_functions_and_expressions()
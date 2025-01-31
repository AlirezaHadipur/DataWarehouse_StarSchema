import pandas as pd
import pyodbc

# Read your dataset
dataset = pd.read_csv('MovieReview.csv')

# Correct the column names to match your DataFrame
Dim_Movies = dataset[['MovieID', "User's Rating out of 10"]].rename(columns={"User's Rating out of 10": "UserRating"})
Dim_Users = dataset[['user_id', 'User', 'Total Votes']].rename(columns={'user_id': 'UserID', 'User': 'UserName', 'Total Votes': 'TotalVotes'})
Dim_Review = dataset[['ReviewID', 'Review', 'TokenCount']]
Dim_Fact = dataset[['ReviewID', 'MovieID', 'user_id', 'Day', 'Month', 'Year']].rename(columns={'user_id': 'UserID'})

# Define the connection string
conn_str = (
    'DRIVER={SQL Server};'
    'SERVER=DESKTOP-PSV5L2B\SQLEXPRESS;'
    'DATABASE=DSS_DataWareHouse;'
    'Trusted_Connection=yes;'
)

# Connect to the SQL Server database
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Function to convert numpy data types to native Python data types
def convert_to_python_type(value):
    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return value.to_pydatetime()
    elif isinstance(value, (pd.Series, pd.DataFrame)):
        return value.to_list()
    else:
        return value.item() if hasattr(value, 'item') else value

# Insert data into Dim_Movies table without duplicates
unique_movies = Dim_Movies.drop_duplicates(subset=['MovieID'])
for index, row in unique_movies.iterrows():
    cursor.execute('''
        INSERT INTO Dim_Movies (MovieID, UserRating)
        VALUES (?, ?)
    ''', convert_to_python_type(row['MovieID']), convert_to_python_type(row['UserRating']))

# Insert data into Dim_Users table without duplicates
unique_users = Dim_Users.drop_duplicates(subset=['UserID'])
for index, row in unique_users.iterrows():
    cursor.execute('''
        INSERT INTO Dim_Users (UserID, UserName, TotalVotes)
        VALUES (?, ?, ?)
    ''', convert_to_python_type(row['UserID']), row['UserName'], convert_to_python_type(row['TotalVotes']))

# Insert data into Dim_Review table without duplicates
unique_reviews = Dim_Review.drop_duplicates(subset=['ReviewID'])
for index, row in unique_reviews.iterrows():
    cursor.execute('''
        INSERT INTO Dim_Review (ReviewID, Review, TokenCount)
        VALUES (?, ?, ?)
    ''', convert_to_python_type(row['ReviewID']), row['Review'], convert_to_python_type(row['TokenCount']))

# Insert data into Fact_Reviews table only if MovieID exists in Dim_Movies
for index, row in Dim_Fact.iterrows():
    cursor.execute('''
        IF EXISTS (SELECT 1 FROM Dim_Movies WHERE MovieID = ?)
        BEGIN
            INSERT INTO Fact_Reviews (ReviewID, MovieID, UserID, Day, Month, Year)
            VALUES (?, ?, ?, ?, ?, ?)
        END
    ''', convert_to_python_type(row['MovieID']), convert_to_python_type(row['ReviewID']), convert_to_python_type(row['MovieID']), convert_to_python_type(row['UserID']), convert_to_python_type(row['Day']), row['Month'], convert_to_python_type(row['Year']))

# Commit the transaction
conn.commit()

# Close the connection
cursor.close()
conn.close()


-- Dimension Tables
CREATE TABLE Dim_Movies (
    MovieID INT PRIMARY KEY,
	UserRating INT
);

CREATE TABLE Dim_Users (
    UserID INT PRIMARY KEY,
    UserName NVARCHAR(100),
	TotalVotes INT
);

CREATE TABLE Dim_Review (
    ReviewID INT PRIMARY KEY,
	Review text,
	TokenCount INT,
);

-- Fact Table
CREATE TABLE Fact_Reviews (
    ReviewID INT,
    MovieID INT,
    UserID INT,
    TotalVotes INT,
    Day INT,
    Month NVARCHAR(20),
    Year INT
    FOREIGN KEY (MovieID) REFERENCES Dim_Movies(MovieID),
    FOREIGN KEY (UserID) REFERENCES Dim_Users(UserID),
    FOREIGN KEY (ReviewID) REFERENCES Dim_Review(ReviewID)
);


select * from test.location;
SELECT test.category.IndustryID, test.category.ID, test.category.Name, count(*) FROM test.category, test.location where test.category.ID = test.location.CategoryID group by test.location.CategoryID;
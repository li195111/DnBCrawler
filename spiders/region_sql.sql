SELECT * FROM test.region;
SELECT test.category.IndustryID, test.category.ID, test.category.Name, count(*) FROM test.category, test.region where test.category.ID = test.region.CategoryID group by test.region.CategoryID;
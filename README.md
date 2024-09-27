# Here is my documentation about my Case solution

I took the decision to use Databricks, after some studies, I thought that it could be a very interesting way to resolve the case, because all my code, data and workflow could be managed in only one "platform".
It was a nice way to understand a new solution too, because I never worked with this tecnology yet, so this could be a huge opportunity to learn something new, in practice.

So, after this decision, I had to setup a Cloud Provider, and I choose Azure for this, because I had 14 days of free trial with 200 dollars of credit. 
Then, I had to create an account on Azure Databricks, to get this free trial, and to could use the Databricks Workflows solution, because this solution is not available to use in the Community Edition(That was like "my development workspace" before I realized it).

So, to run the code, locally, in this solution, it gets very simple, because you only need to go to Workflows tab, select Breweries Pipeline, and click on the button in the right corner in Run Now. But, it is because I get all the thing setted up(schemas, file paths, volumes).

Now, thinking that you will do this solution in another machine, I will describe the steps that I took to setup the environment for you too! 

###### Note: To replicate 100% percent of this solution, I think that you will need a Databricks account too.
1. Clone this repository in your machine.
2. Open your Databricks interface.
3. Open the Workspace tab.
4. Go to Users.
5. Select your user email.
6. Click the three dots in right corner(on the top) and select Import.
7. Then select "Import from: File" and select the Brewery_API folder(it will be inside the repository that you cloned in the step 1).

> After that, I recommend that you set up all the resouces like volumes and schemas that we need to use to replicate this solution. So let's do it:

8. Go to Catalog tab and select a Catalog of you prefference.

> Inside this Catalog you will need to do this actions:

9. Create three schemas by clicking in the button in the right side. The names need to bee bronze_layer, silver_layer and gold_layer.
10. With the three schemas created, you will enter in the bronze_layer and create a volume(by clicking in the create button and selecting create volume) called breweries_api.

> Now, I thinkt that all the resources needed to run the code almost exactly as it is in my notebooks.

> With all this steps done, it's time to finally run the code! 

13. Return to Workspace tab, open the Brewery API folder

> The files are already in the order that need to be run to fill all the three layers. So let`s do it. 

14. Open the Brewery_API_Ingestion notebook
15. Go to the last cell and edit the last line to it:
- dbutils.fs.put('/Volumes/{folder}/bronze_layer/breweries_api/breweries_data.json', breweries, overwrite=True)
> substituing {folder} to the Catalog from setp 8.
> ex: if your catalog from setp was called test, it will be:
- dbutils.fs.put('/Volumes/test/bronze_layer/breweries_api/breweries_data.json', breweries, overwrite=True)

16. Now you can click in Run All button in the rigth corner. This step will create a json file in our volume(like a storage) that could be our bronze layer of the medallion architecture.

17. Return to Brewery API folder and open the second notebook(Brewery_Bronze_to_Silver).
18. In the first cell you will need to replace the path in the second line.

- bronze_data = spark.read.json('/Volumes/flavioteste/bronze_layer/breweries_api/breweries_data.json')
> You will need to use the same path passed in step 15.
> Using the example of the step 15 -> '/Volumes/teste/bronze_layer/breweries_api/breweries_data.json' the line will be like that:
- bronze_data = spark.read.json('/Volumes/teste/bronze_layer/breweries_api/breweries_data.json')

19. Click in Run All button. This action will create the silver layer, this layer is already "queryable"(you could do SQL queries on it).
20. Return to Brewery API folder and open the third notebook(Brewery_Silver_to_Gold).
21. In this notebook you only need to click in Run All button. 
22. With all the steps you will have all the three layer filled with data. To confirm it, you could go to Catalog and search the tables and files in all the schemas that you created in the step 9. 

## Considerations:
- You will need to attach some cluster to could run all the notebooks. 
- If you want to make this pipeline automated, you could use the Workflows feature. We could see this together, but you could create three tasks, one for every notebook(except the Test notebook).
- The test notebook(Test_and_Quality_Check) you could use to make queries, and see some quality checks. 
- The code from api extraction could get better avoiding for inside another for. 
- The bronze layer could get better, it could save a file per day, to could have a history of extractions.
- With the workflows solution, we could get notified if the pipeline fails we could use email or any notification channel that we preffer that have an integration with Databricks. 
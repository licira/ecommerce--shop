# ecommerce--shop

### 5.Product matching, enriching existing products, adding new products

When performing product matching we have to consider a field as an acting identifier, in the sense
that when new data arrives we would know that it corresponds to a an existing record or a new one.
As it can be noticed the "id" field is from the supplier_data.json is a good candidate for this.

Therefore, when matching that a full outer join operation between the existing data and the new data would enable the 
matching between the records. For the case when new data won't have a match we can simply add that data to the target data.
If there is a match between the records from the two datasets we could overwrite the dataset record corresponding to old data
with new data from the incoming dataset, this way we would update it with new information and then save id in the target
along with newly added data.

Challenges that might come along (with proposed solution):
1. Existing data grows and it brings along performance issues:
- We could filter by id the existing target data before performing the join such that
we would only perform a one side outer join to keep only new records along with ones to update and don't 
work with the ones that do not require any change

2. Loss of historic state fo data
- if we override the existing target data we lose information on the changes that data encountered through time.
In case this is of interest, the solution would be to create a new target dataset each time we perform step 5.

### Setup

Import the current project into Intellij (or eclipse).

Set Java 1.8 as project SDK.

Set Java 1.8 as programming language level.

Set target bytcode level 8 for Java compiler.

Create run configuration with: "
Main class: SparkPipeline, 
Program arguments: src/test/resources/supplier_car.json <output_path>

Hit "Apply".

Run the newly created run configuration

The successful run will provide 4 folders with each step of the processing, as requested.

The current job can also be run by using spark 2.3.0 and performing a spark-submit-2.3.0 with the shaded jar as an argument.

To obtain the shaded jar run 
// Print excution time for different queries
// Based on transformed data formats (new data is stored in transaction_data_t)
// use Amazon_t as our new database
db.transaction_data_t.createIndex({ "product_info.asin": 1 });


// Query 1: 10211 ms
var startTime = new Date();

var results = db.transaction_data_t.aggregate([
  {
    $group: {
      _id: "$product_info.asin",
      totalSales: { $sum: "$copy" },
      title: { $first: "$product_info.title" },
      price: { $first: "$product_info.price" }
    }
  },
  { $sort: { totalSales: -1 } },
  { $limit: 10 },
  {
    $project: {
      _id: 0,
      asin: "$_id",
      totalSales: 1,
      title: 1,
      description: 1,
      price: 1
    }
  }
]).toArray();

var endTime = new Date();
var queryTime = endTime - startTime;

printjson(results);
print('Query running time: ' + queryTime + ' ms');




// Query 2: 11532 ms
var startTime = new Date();

var results = db.transaction_data_t.aggregate([
  { 
    $group: { 
      _id: "$product_info.asin", 
      averageRating: { $avg: "$overall" },
      title: { $first: "$product_info.title" },
      price: { $first: "$product_info.price" }
    } 
  },
  { $sort: { averageRating: -1 } },
  { $limit: 10 },
  { 
    $project: { 
      _id: 0, 
      asin: "$_id", 
      averageRating: 1, 
      title: 1, 
      price: 1
    } 
  }
]).toArray();

var endTime = new Date();
var queryTime = endTime - startTime;

printjson(results);
print('Query running time: ' + queryTime + ' ms');



// Query 3: 3472 ms
var targetUserID = "A2G6MU2JV29206";
var startTime = new Date();

var results = db.transaction_data_t.aggregate([
  { $match: { reviewerID: targetUserID } },
  { $unwind: "$product_info.also_buy" },
  { $group: { _id: "$reviewerID", also_buy: { $addToSet: "$product_info.also_buy" } } },
  { $project: { _id: 0, also_buy: 1 } },
  { $unwind: "$also_buy" },
  { $sample: { size: 6 } } // Randomly select 6 items from the also_buy array
]).toArray();

var endTime = new Date();
var queryTime = endTime - startTime;

printjson(results);
print('Query running time: ' + queryTime + ' ms');



// Query 4: 3481 ms
var targetUserID = "A2G6MU2JV29206";
var startTime = new Date();

var results = db.transaction_data_t.aggregate([
  { $match: { reviewerID: targetUserID } },
  { $unwind: "$product_info.also_view" },
  { $group: { _id: "$reviewerID", also_view: { $addToSet: "$product_info.also_view" } } },
  { $project: { also_view: 1, _id: 0 } },
  { $unwind: "$also_view" },
  { $sample: { size: 6 } } // Randomly select 6 items from the also_view array
]).toArray();

var endTime = new Date();
var queryTime = endTime - startTime;

printjson(results);
print('Query running time: ' + queryTime + ' ms');



// Query 5: 10302 ms
var startTime = new Date();

var results = db.transaction_data_t.aggregate([
  {
    $group: {
      _id: "$product_info.asin",
      totalSales: { $sum: "$copy" }, 
      title: { $first: "$product_info.title" },
      price: { $first: "$product_info.price" } 
    }
  },
  { $sort: { totalSales: -1 } }, 
  { $limit: 10 },
  {
    $project: {
      _id: 0,
      asin: "$_id", 
      totalSales: 1,
      title: 1, 
      price: 1 
    }
  }
]).toArray();

var endTime = new Date();
var queryTime = endTime - startTime;

printjson(results);
print('Query running time: ' + queryTime + ' ms');



// Query 6: 11464 ms
var startTime = new Date();

var results = db.transaction_data_t.aggregate([
  { 
    $group: { 
      _id: "$product_info.asin", 
      averageRating: { $avg: "$overall" },
      title: { $first: "$product_info.title" },
      price: { $first: "$product_info.price" }
    }
  },
  { $sort: { averageRating: -1 } }, 
  { $limit: 10 },
  { 
    $project: { 
      _id: 0, 
      asin: "$_id", 
      averageRating: 1, 
      title: 1, 
      price: 1 
    } 
  }
]).toArray();

var endTime = new Date();
var queryTime = endTime - startTime;

printjson(results);
print('Query running time: ' + queryTime + ' ms');
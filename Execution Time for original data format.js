// Print excution time for different queries
// Based on original data formats

// Query 1: 21574 ms
var startTime = new Date();

var results = db.transaction_data.aggregate([
  { $group: { _id: "$asin", totalSales: { $sum: "$copy" } } },
  { $sort: { totalSales: -1 } },
  { $limit: 10 },
  { $lookup: { from: "meta_data", localField: "_id", foreignField: "asin", as: "product_info" } },
  { $unwind: "$product_info" },
  { $project: { _id: 0, totalSales: 1, product_info: { asin: "$_id", title: "$product_info.title", description: "$product_info.description" } } }
]).toArray();

var endTime = new Date();
var queryTime = endTime - startTime;
printjson(results);
print('Query running time: ' + queryTime + ' ms');


// Query 2: 17836 ms
var startTime = new Date();

var results = db.review_data.aggregate([
  { $group: { _id: "$asin", averageRating: { $avg: "$overall" } } },
  { $sort: { averageRating: -1 } },
  { $limit: 10 },
  { $lookup: { from: "meta_data", localField: "_id", foreignField: "asin", as: "product_info" } },
  { $unwind: "$product_info" },
  { $project: { averageRating: 1, product_info: { asin: "$_id", title: "$product_info.title", description: "$product_info.description" } } }
]).toArray();

var endTime = new Date();
var queryTime = endTime - startTime;
printjson(results);
print('Query running time: ' + queryTime + ' ms');



// Query 3: 24205 ms
var targetUserID = "A2G6MU2JV29206";
var startTime = new Date();

var results = db.transaction_data.aggregate([
  { $match: { reviewerID: targetUserID } },
  { $lookup: { from: "meta_data", localField: "asin", foreignField: "asin", as: "product_info" } },
  { $unwind: "$product_info" },
  { $group: { _id: "$reviewerID", also_buy: { $push: "$product_info.also_buy" } } },
  { $project: { also_buy: { $reduce: { input: "$also_buy", initialValue: [], in: { $concatArrays: ["$$value", "$$this"] } } } } },
  { $unwind: "$also_buy" },
  { $unwind: "$also_buy" },
  { $addFields: { randomField: { $rand: {} } } },
  { $sort: { randomField: 1 } },
  { $limit: 6 },
  { $lookup: { from: "meta_data", localField: "also_buy", foreignField: "asin", as: "product_details" } },
  { $unwind: "$product_details" },
  { $project: { product_info: { asin: "$also_buy", title: "$product_details.title", description: "$product_details.description" } } }
]).toArray();

var endTime = new Date();
var queryTime = endTime - startTime;
printjson(results);
print('Query running time: ' + queryTime + ' ms');


// Query 4: 21792 ms
var targetUserID = "A2G6MU2JV29206"; 
var startTime = new Date(); 

var results = db.transaction_data.aggregate([
  { $match: { reviewerID: targetUserID } },
  { $lookup: { from: "meta_data", localField: "asin", foreignField: "asin", as: "product_info" } },
  { $unwind: "$product_info" },
  { $group: { _id: "$reviewerID", also_view: { $push: "$product_info.also_view" } } },
  { $project: { also_view: { $reduce: { input: "$also_view", initialValue: [], in: { $concatArrays: ["$$value", "$$this"] } } } } },
  { $unwind: "$also_view" },
  { $unwind: "$also_view" },
  { $addFields: { randomField: { $rand: {} } } },
  { $sort: { randomField: 1 } },
  { $limit: 10 },
  { $lookup: { from: "meta_data", localField: "also_view", foreignField: "asin", as: "product_details" } },
  { $unwind: "$product_details" },
  { $project: { product_info: { asin: "$also_view", title: "$product_details.title", description: "$product_details.description" } } }
]).toArray();

var endTime = new Date(); 
var queryTime = endTime - startTime; 

printjson(results);
print('Query running time: ' + queryTime + ' ms');

// Query 5: 15563 ms
var startTime = new Date();

var results = db.transaction_data.aggregate([
  { $group: { _id: "$asin", totalSales: { $sum: 1 } } },
  { $sort: { totalSales: -1 } },
  { $limit: 10 },
  { $lookup: { from: "meta_data", localField: "_id", foreignField: "asin", as: "product_info" } },
  { $unwind: "$product_info" },
  { $project: { asin: "$_id", totalSales: 1, title: "$product_info.title", description: "$product_info.description" } }
]).toArray();

var endTime = new Date();
var queryTime = endTime - startTime;

printjson(results);
print('Query running time: ' + queryTime + ' ms');

// Query 6: 18604 ms
var startTime = new Date();

var results = db.review_data.aggregate([
  { $group: { _id: "$asin", averageRating: { $avg: "$overall" } } },
  { $sort: { averageRating: -1 } },
  { $limit: 10 },
  { $lookup: { from: "meta_data", localField: "_id", foreignField: "asin", as: "product_info" } },
  { $unwind: "$product_info" },
  { $project: { asin: "$_id", averageRating: 1, title: "$product_info.title", description: "$product_info.description" } }
]).toArray();

var endTime = new Date();
var queryTime = endTime - startTime;

printjson(results);
print('Query running time: ' + queryTime + ' ms');

// Query 7
// Based on total Voters: 1050 ms
var startTimeVotes = new Date();

var topVoters = db.user_data.aggregate([
  { $project: { reviewerID: 1, reviewerName: 1, totalVotes: 1 } },
  { $sort: { totalVotes: -1 } },
  { $limit: 10 }
]).toArray();

var endTimeVotes = new Date();
var queryTimeVotes = endTimeVotes - startTimeVotes;

printjson(topVoters);
print('Top Voters Query running time: ' + queryTimeVotes + ' ms');

// Based on total Voters: 836 ms
var startTimeFans = new Date();

var topFans = db.user_data.aggregate([
  { $project: { reviewerID: 1, reviewerName: 1, fans: 1 } },
  { $sort: { fans: -1 } },
  { $limit: 10 }
]).toArray();

var endTimeFans = new Date();
var queryTimeFans = endTimeFans - startTimeFans;

printjson(topFans);
print('Top Fans Query running time: ' + queryTimeFans + ' ms');

// Query 8: 3963 ms
var startDate = new Date("2012-01-01").getTime();
var endDate = new Date("2012-01-05").getTime();
var startTime = new Date();  

var transactions = db.transaction_data.find({
  "transactionTime": {
    $gte: startDate,
    $lte: endDate
  }
}).toArray();

var endTime = new Date();  
var queryTime = endTime - startTime; 

printjson(transactions);
print('Query running time: ' + queryTime + ' ms');


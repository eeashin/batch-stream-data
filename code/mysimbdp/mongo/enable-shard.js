db = db.getSiblingDB("admin");
db.auth("admin", "admin");
sh.enableSharding("batch_airbnb");
sh.shardCollection("batch_airbnb.reviews", { "listing_id_id_reviewer_id": "hashed" });
sh.enableSharding("stream_airbnb");
sh.shardCollection("stream_airbnb.reviews", { "listing_id_id_reviewer_id": "hashed" });
sh.enableSharding("batch_zoo");
sh.shardCollection("batch_zoo.tortoise", { "time": "hashed" });
sh.enableSharding("stream_zoo");
sh.shardCollection("stream_zoo.tortoise", { "time": "hashed" });
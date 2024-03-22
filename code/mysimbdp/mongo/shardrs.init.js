rs.initiate({
    _id: 'mongo-shard-01',
    members: [
      { _id: 0, host: 'mongo-shard-1:27017', priority: 1 }, 
      { _id: 1, host: 'mongo-shard-2:27017', priority: 0 } 
    ]
  })
  
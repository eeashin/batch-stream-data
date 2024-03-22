rs.initiate({
    _id: 'mongo-configserver',
    configsvr: true,
    version: 1,
    members: [
      {_id: 0, host: 'mongo-config:27017'}
    ]
  })  
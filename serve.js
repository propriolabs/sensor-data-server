var express = require('express');
var app = express();
var elasticsearch = require('elasticsearch');

var request = require('request');

var config = require('./config');

var async = require('async');

var _ = require('underscore');

var fs = require('fs');

var options = {
   key  : fs.readFileSync(config.server_key),
   cert : fs.readFileSync(config.server_cert)
};

var server = require('https').createServer(options,app).listen(config.server_port);
var io = require('socket.io').listen(server);
var esclient = new elasticsearch.Client({
  host: config.es_host,
});

Array.prototype.sum = function() {
    return this.reduce(function(a,b){return a+b;});
};

function getAnalyzedForAll(callback){
  esclient.search({
        index: config.es_index,
        type: config.es_analysis_type,
        body: {
          size : 100000,
          query : {
            match_all : {
            }
          }
        }
    }).then(function (resp) {
      var guys = {};
       _.map(resp.hits.hits,function(x){ guys[x._source.session] = x._source; });
      callback(null,guys);
    }, function (err) {
      console.trace(err.message);
      callback(null,[]);
    });
}


function averageHeartrate(last){
  console.log(last.session);
  console.log(last.userId);
  esclient.search({
     index: config.es_index,
        type: config.es_type,
        body: {
          size : 999999,
          sort : [{ watchtime : {order : "asc"}}],
          query: {
                bool: {
                    must: [
                        {match: {session: last.session}},
                        {match: {userId: last.userId}}
                        ]
        }
      }
    }
    }).then(function (resp) {
      console.log('Returned result on disconnect');
      var hr=_.map(resp.hits.hits,function(x){return [x._source.watchtime,x._source.heartrate]});
      var avghr=[];
      for(var i=hr[0][0]+30000;i<hr[hr.length-1][0];i+=30000){
        var sliced=hr.filter(function(x){return x[0]<i & x[0]>i-30000})
        var hrslice=sliced.map(function(x){return x[1]})
        avghr.push({'hr':Math.round(hrslice.sum()/hrslice.length),'watchtime':i})
      }
      console.log(avghr)
      async.parallel([
           getSessionId.bind({'session':last.session,'userId':last.userId})
           ], function(error, results) {
              var esid=results[0];
              esclient.update({index: config.es_index,
                type: config.es_analysis_type,
                id: esid,
                body:{
                 doc : {
                    heartrate: avghr
                    }
                }
              })
                  })
     });

}

function getSessionId(callback){
   esclient.search({
        index: config.es_index,
        type: config.es_analysis_type,
        body: {
          size : 1000,
          query: {
                bool: {
                    must: [
                        {match: {session: this.session}},
                        {match: {userId: this.userId}}
                        ]
                    }
            }
      }
    }).then(function(resp){
        console.log(resp.hits.hits[0]._id)
        id=resp.hits.hits[0]._id
        callback(null,id);
    }, function (err) {
      console.trace(err.message);
      console.log("ERROR")
      callback(null,[]);
   
    });
    }


function getAnalyzedForUser(callback){
  esclient.search({
        index: config.es_index,
        type: config.es_analysis_type,
        body: {
          size : 1000,
          query : {
            match : {
              userId:this
            }
          }
        }
    }).then(function (resp) {
      var guys = {};
       _.map(resp.hits.hits,function(x){ guys[x._source.session] = x._source; });
      callback(null,guys);
    }, function (err) {
      console.trace(err.message);
      callback(null,[]);
    });
}

function getAllUserData(callback){
  console.log(this);
  esclient.search({
        index: config.es_index,
        type: config.es_analysis_type,
        body: {
          size : 1000,
          sort : [{ session : {order : "desc"}}],
          query : {
            match : {
              userId:this
            }
          }
        }
    }).then(function (resp) {
      var guys = [];
       _.map(resp.hits.hits,function(x){ guys.push(x._source); });
      callback(null,guys);
    }, function (err) {
      console.trace(err.message);
      callback(null,[]);
    });
}

function getWeatherandGPS(callback){
  console.log(this);
  esclient.search({
        index: config.es_index,
        type: config.es_session_type,
        body: {
          size : 1000,
          sort : [{ sessionId : {order : "desc"}}],
          query : {
            match : {
              userId:this
            }
          }
        }
    }).then(function (resp) {
      var guys = [];
       _.map(resp.hits.hits,function(x){ guys.push(x._source); });
      callback(null,guys);
    }, function (err) {
      console.trace(err.message);
      callback(null,[]);
    });
}





function getGlobalData(callback){
  esclient.search({
        index: config.es_index,
        type: config.es_analysis_type,
        body: {
          size : 200,
          sort : [{ session : {order : "desc"}}],
          query : {
            match : {"privacy": "Yes"}
          }
        }
    }).then(function (resp) {
      var guys = [];
       _.map(resp.hits.hits,function(x){ guys.push(x._source); });
      callback(null,guys);
    }, function (err) {
      console.trace(err.message);
      callback(null,[]);

    });
}

function getUserRankings(callback){
  esclient.search({
        index: config.es_index_rankings,
        type: config.es_rank_type,
        body: {
          size : 10000,
          query : {
            match : {
              userId:this
            }
          }
        }
    }).then(function (resp) {
    	console.log(resp);
      var guys = {};
       _.map(resp.hits.hits,function(x){ guys = x._source; });
      callback(null,guys);
    }, function (err) {
      console.trace(err.message);
      callback(null,null);
    });
}

function getSessionsForAll(callback){
  var userId = this;
  esclient.search({
        index: config.es_index,
        type: config.es_type,
        body: {
          size: 0,
          aggs:{
            not_private:{
              filter:{
                term:{
                  'privacy':"yes"
                }
              },
              aggs: {
                my_sessions: {
                  terms: {
                    field: "session",
                    order: {
                      _term: "desc"
                    },
                    size:0
                  },
                  aggs: {
                    user:{
                      terms:{
                        field:"userId"
                      },
                      aggs: {
                        max_time: {
                          max: {
                            field: "watchtime"
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
    }).then(function (resp) {
      var hits = -1;
      console.log(JSON.stringify(resp));
      callback(null,_.map(resp.aggregations.not_private.my_sessions.buckets, function(x){
        var d = {};
        d['session'] = x.key;
        d['userId'] = x.user.buckets[0].key;
        d['data'] = x.user.buckets[0];
        return d;
      }));
    }, function (err) {
      console.trace(err.message);
      callback(null,[]);

    });
}





function getSessionsForUser(callback){
  var userId = this;
  esclient.search({
        index: config.es_index,
        type: config.es_type,
        body: {
          size: 0,
          aggs: {
            my_sessions: {
              filter: {
                term: {
                  'userId': userId
                }
              },
              aggs: {
                sessions: {
                  terms: {
                    field: "session",
                    order: {
                      _term: "desc"
                    },
                    size:0
                  },
                  aggs: {
                    user:{
                      terms:{
                        field:"userId"
                      },
                      aggs: {
                        max_time: {
                          max: {
                            field: "watchtime",
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
        }
      }
    }).then(function (resp) {
      var hits = resp.aggregations.my_sessions.doc_count;
      callback(null,_.map(resp.aggregations.my_sessions.sessions.buckets, function(x){
        var d = {};
        d['session'] = x.key;
        d['userId'] = userId;
        d['data'] = x.user.buckets;
        return d;
      }));
    }, function (err) {
      console.trace(err.message);
      callback(null,[]);
    });
}

io.sockets.on('connection', function(client){
     console.log("connection from client...");
     client.on('sessions',function(user){
        
        if(user.userId != null) {
          console.log("sessions call...:" + user.userId);
          async.parallel([
              getSessionsForUser.bind(user.userId),
              getAnalyzedForUser.bind(user.userId)
           ], function(error, results) {
                
                var sessions = results[0];
                var analysis = results[1];
                var good = _.sortBy(_.map(sessions,function(x){
                  y = {}
                  y.analysis = null;
                  y.session = x.session;
                  y.samples = x.data[0].doc_count;
                  y.max_time = x.data[0].max_time.value;
                  y.userId = x.userId;

                  if(x.session in analysis) {
                    y.analysis = analysis[y.session];
                    y.userName = y.analysis.userName;
                  }
                  return y;
                }),function(x){ return x.max_time.value;});
                console.log(JSON.stringify(good));
                client.emit('sessions', {'data': good});
              });
      }
      else {
        console.log("sessions call...:none");
          async.parallel([
              getSessionsForAll,
              getAnalyzedForAll
           ], function(error, results) {
                
                var sessions = results[0];
                var analysis = results[1];
                var good = _.sortBy(_.map(sessions,function(x){
                  y = {}
                  y.analysis = null;
                  y.session = x.session;
                  y.samples = x.data.doc_count;
                  y.max_time = x.data.max_time.value;
                  y.userId = x.userId;

                  if(x.session in analysis) {
                    y.analysis = analysis[y.session];
                    y.userName = y.analysis.userName;
                  }
                  return y;
                }),function(x){ return x.max_time;});
                //console.log(JSON.stringify(good));
                client.emit('sessions', {'data': good});
              });
      }
          
      });


    client.on('userStats',function(user){
        console.log(user)
        if(user.userId != null) {
          console.log("userStats for " + user.userId);
          async.parallel([
              getAllUserData.bind(user.userId),
              getWeatherandGPS.bind(user.userId)

           ], function(error, results) {
                var analysis = results[0];
                var sessions = results[1];
                console.log(JSON.stringify(analysis))
                console.log(JSON.stringify({"meta": sessions}))

                client.emit('userStats', {'data': analysis, 'meta': sessions});
              });
      }
      else if(user != null) {
          console.log("userStats for ... progressfragment ..." + user);
          async.parallel([
              getAllUserData.bind(user)
           ], function(error, results) {
                var analysis = results[0];
                console.log(JSON.stringify(analysis))
                client.emit('userStats', {'data': analysis});
              });
      }
      else {
        console.log("No User Id, what???");
      }
          
      });

    client.on('GlobalStats',function(user){
        
        if(user.userId != null) {
          console.log("GLOBALStats from" + user.userId);
          async.parallel([
              getGlobalData.bind(user.userId)
           ], function(error, results) {
                var analysis = results[0];
                console.log(JSON.stringify(analysis))
                client.emit('GlobalStats', {'data': analysis});
              });
      }
      else {
        console.log("No User Id, what???");
      }
          
      });


  client.on('userRanks',function(user){
    
        if(user.userId != null) {
          console.log("userRanks for " + user.userId);
          async.parallel([
              getUserRankings.bind(user.userId)
           ], function(error, results) {
                var analysis = results[0];
                console.log(JSON.stringify({'rankdata': analysis}));
                client.emit('userRanks', {'rankdata': analysis});
              });
      }
      else {
        console.log("No User Id, what???");
      }
          
      });


  client.on('analyze',function(session) {
    console.log("analyze");
    client.session=session.session;
    client.userId=session.googleUserId;
    var sess = session.session;
    var userId = session.googleUserId;
    var userName = session.googleUserName;
    request({
          url: config.analysis_url,
          method: 'POST',
          json: {
              'session': sess,
              'userId': userId
          }
      }, function(error, response, body){
          if(error) {
              console.log(error);
          } else {
              console.log(response.statusCode, body);
              body.userName = userName;
              esclient.index({
                index: config.es_index,
                type: config.es_analysis_type,
                id: sess, 
                body: body
              }, function (error, response) {
                //console.log(response);
              });
              client.emit('analyze_results', {'data': body});
      }
    });

    
  });

  client.on('get_session',function(session) {
    var sess = session;
    console.log(session);
    esclient.search({
        index: config.es_index,
        type: config.es_analysis_type,
        body: {
          query:{
            match:{
              _id:sess
            }
          }
        }
    }).then(function (resp) {
      client.emit('session_data', {'data': resp.hits.hits[0]._source});
    }, function (err) {
      console.trace(err.message);
    });
  });

  client.on('new_user',function(user) {
    esclient.index({
      index: config.es_index,
      type: config.es_users_type,
      id: user.userId, 
      body: user
    }, function (error, response) {
      console.log(response);
    });
  });

  client.on('set_score',function(score) {
        console.log('set_score ',score);
        esclient.index({
          index: config.es_index,
          type: config.es_score_type,
          body: score
        }, function (error, response) {
          console.log(response);
        });

        client.emit('data-ack', {});
       
  });

  client.on('audio_label',function(label) {
        console.log('audio_label ',label);
        esclient.index({
          index: config.es_index,
          type: config.es_audio_labels_type,
          body: label
        }, function (error, response) {
          console.log(response);
        });

        client.emit('data-ack', {});
       
  });

  client.on('new_session',function(session) {
        console.log('new_session ',session);
        esclient.index({
          index: config.es_index,
          type: config.es_session_type,
          body: session
        }, function (error, response) {
          console.log(response);
        });

        client.emit('data-ack', {});
       
  });
     
  client.on('message',function(event) {
        if (event instanceof Array) {
            console.log('Received values from client! ',event.length);
            for (var i = 0; i < event.length; i++) {
    	        esclient.index({
    	 	        index: config.es_index,
    	  	      type: config.es_type,
    	  	      body: event[i]
    	      	}, function (error, response) {
    		      });
        	}
        }
        else if(event instanceof Object) {
          console.log('Received val from client!' + JSON.stringify(event));
              esclient.index({
                index: config.es_index,
                type: config.es_type,
                body: event
              }, function (error, response) {
                console.log(response);
              });
        }

        client.emit('data-ack', {});
       
  });
    
  client.on('disconnect',function() {
      if(typeof this.session!=="undefined" & typeof this.userId!=="undefined"){
        averageHeartrate({'session':this.session,'userId':this.userId})
        console.log("just finished analyzing")

      }
      console.log('Server has disconnected');
  });

  client.on('ping',function(){
      console.log("ping...");
      client.emit('ping_response',{'response':'up'});
  });

});




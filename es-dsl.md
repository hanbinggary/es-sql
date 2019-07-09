# some elasticsearch dsl  <=> sql

## limit 0,10

{
    "from":0,"size":10
    #defalut 10,max index.max_result_window=10000
}

...

## order by name desc,age asc

{
    "sort" : [
        { "name" : "desc" },
        { "age" : "asc" },
    ]
}

The order defaults to desc when sorting on the _score, and defaults to asc when sorting on anything else.

...

## select username from ...

{
    "_source": ["username"]
}

...

## scroll

url : index_name/_search?scroll=1m
{
    "size": 3, # max return
    "_source": [
        "info_time"
    ],
    "sort": [
        "info_time"
    ]
}

url: _search/scroll
{
    "scroll" : "1m",
    "scroll_id" : "xxxx"
}

...

## == != > < and or not etc
{"term":{"username":"cj"}} # ==
{"terms":[{"username":"cj"}]} # in
{"exists":{"field":"username"}} # is not null
{"missing":{"field":"username"}} # is null
{"wildcard":{"username":"*cj*"}} # like(not analyzed)

{
    "must":[]        # and
    "must_not":[]    #and not
    "should":[]      # or

...

## group by

{
  "aggs": {
    "group_by_state": {
      "terms": {
        "field": "state.keyword"
      }
    }
  }
}
OR
{
  "aggs": {
    "state": {
      "terms": {
        "field": "state"
      },
      "aggs": {
        "name":{
            "terms": {
                "field":"name"
            }
        }
      }
    }
  }
}

...

## having

{
    "bucket_selector":{
        "buckets_path":{
            "val_100": "count"
        },
        "script": "(val_100 == 6862)"
    }
}


# delete

only es5+ supported

# update

# insert

# create
{
    #default 5/1
    "settings" : {
        "number_of_shards" : 5,
        "number_of_replicas" : 1
    },
    "mappings" : {
        "type1" : {
            "properties" : {
                "field1" : { "type" : "text" }
            }
        }
    }
}

field type
string: text/keyword
numeric: long/integer/short/byte/double/float/half_float/scaled_fload
date: date
boolean: boolean
binary: binary
range: integer_range/float_range/long_range/double_range/date_range
....
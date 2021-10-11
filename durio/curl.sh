curl -i -X POST -H 'Content-Type: application/json' -d '{"key": "hi", "value": "Hello "}' http://10.1.1.51:9007/kvstore/put
curl -i -X GET http://10.1.1.56:9008/kvstore/get/hi
curl -i -X POST -H 'Content-Type: application/json' -d '{"key": "hi", "value": "World!"}' http://10.1.1.198:9006/kvstore/append
curl -i -X GET http://10.1.1.51:9007/kvstore/get/hi
curl -i -X POST -H 'Content-Type: application/json' -d '{"key": "hi", "value": "      "}' http://10.1.1.56:9008/kvstore/put
curl -i -X GET http://10.1.1.198:9006/kvstore/get/hi

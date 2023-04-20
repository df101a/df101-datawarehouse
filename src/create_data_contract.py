from schema_registry.client import SchemaRegistryClient, schema

client = SchemaRegistryClient(url="http://13.93.26.75:8081")

deployment_schema = {
    "definitions" : {
        "JsonDeployment" : {
            "type" : "object",
            "required" : ["image", "replicas", "port"],
            "properties" : {
                "token" :       {"type" : "string"},
                "value" :       {"type" : "number"},
                "timestampz" :  {"type" : "integer"},
                "source" :      {"type" : "string"}
            }
        }
    },
    "$ref" : "#/definitions/JsonDeployment"
}

json_schema = schema.JsonSchema(deployment_schema)

schema_id = client.register("standard-schema", json_schema)

print(schema_id)
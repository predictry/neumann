#Neumann
Service Provider: Neo4j

##API

###Services

  - Endpoint: /services
  
```javascript
{
    "name": str,
    "tenant": str,
    "output": {
        "store": str,
        "fields": <K, V>
    }
}
```

Sample:
```javascript
{
    "name": "Compute-Recommendation",
    "tenant": "Store-A",
    "output": {
        "store": "S3",
        "fields": {
	        "bucket": "predictry",
	        "path": "data/reco"
        }
    }
}
```

##Notes

###*On the subject of* **items categories**
Some categories contain special characters, which are not filesystem friendly. For such categories,
these special characters should be replaced with an underscore. This rule does not apply to spaces,
as they should be filesystem friendly.

*Note: This currently applies to the `/` character.*
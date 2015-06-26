```Delete Invalid Data
MATCH (n :`Item` :`Tenant`) WHERE n.id =~ ".*\\[.*" WITH n LIMIT 300 MATCH (n)-[r]-() DELETE r, n;
MATCH (n :`User` :`Tenant`) WHERE n.id =~ ".*\\[.*" WITH n LIMIT 300 MATCH (n)-[r]-() DELETE r, n;
MATCH (n :`Session` :`Tenant`) WHERE n.id =~ ".*\\[.*" WITH n LIMIT 300 MATCH (n)-[r]-() DELETE r, n;
MATCH (n :`Agent` :`Tenant`) WHERE n.id =~ ".*\\[.*" WITH n LIMIT 300 MATCH (n)-[r]-() DELETE r, n;

```

#Cypher Queries for Recommendations Journal

###Who bought this item?

```
MATCH (i :`Item`)<-[r  :`BUY`]-(u :`User`)
WHERE i.id = {item_id}
RETURN u
```

###What shampoos did these users buy?

```
MATCH (i :`Item`)<-[r1  :`BUY`]-(u :`User`)
MATCH (alt :`Item`)<-[r2  :`BUY`]-(u)
WHERE i.id = {item_id} AND alt.description=~ "(?i).*shampoo.*" AND NOT ((i)<-[r2]-(u))
RETURN alt,r2,u
```

**Note**: The word shampoo here is a given. In a real scenario, it would either have to be a tag, or we'd have to extract all words from the first item's description, and find items with a similar description based on some text similarity
scoring algorithm (this would have to be done outside Neo4j, and would be highly expensive) -- Batch process overnight? TF-IDF?

###Get every item purchased by a specific user

```
MATCH (i :`Item`)<-[r1  :`BUY`]-(u :`User`)
WHERE u.id = {user_id}
RETURN i.id AS id, i.description AS Description
```

##Sample products

###Find people that bought 2 products in conjunction

```
MATCH ( :`Item` {id:6028})<-[r1  :`BUY`]-(u :`User`),( :`Item` {id:5728})<-[r2  :`BUY`]-(u)
RETURN u, r1, r2
```

###Find (distinct) purchases from people that bought 2 products in conjunction

```
MATCH ( :`Item` {id:6028})<-[r1  :`BUY`]-(u :`User`),( :`Item` {id:5728})<-[r2  :`BUY`]-(u)
MATCH (i :`Item`)<-[r3  :`BUY`]-(u)
WHERE NOT(i.id IN [6028, 5728])
RETURN u.id AS UserId, COLLECT(DISTINCT i.id) AS ItemsPurchased
ORDER BY UserId
LIMIT 100
```

####Find top purchases from people that bought 2 products in conjunction

Count the frequency of each item for the users combined (once per user)
Return the list with the list count
All done on outside neo4j


###Find similar items based on item description

####Get items' descriptions

```
MATCH (i :`Item`) WHERE i.id IN [6028, 5728]
RETURN i.id AS ItemId, i.description AS ItemDescription
```

####Extract words from description (include non-purchased items?)

```
MATCH (i :`Item`)
WHERE i.description =~ "(?i).*bathroom.*"
OR i.description =~ "(?i).*cleaner.*"
AND NOT(i.id IN [6028, 5728])
RETURN i.id AS ItemId, i.description AS ItemDescription
LIMIT 200
```

####Find similar items based on match of description words
?

###Get all checkouts (and items) for a particular product
1. Look up viewed items
2. Group them by their session id
3. I only want the checkouts (sessions) with of the product x, along with other products (basketSize)
4. Give me 100 of these checkouts (sessions) only

```
MATCH (i :`Item`)<-[r :VIEWED]-(u)
WITH r.sessionid AS sessionId, COLLECT(i.id) AS items, COUNT(i.id) AS basketSize
WHERE ANY(item in items WHERE item IN [11270]) AND basketSize > 1
RETURN sessionId, items
LIMIT 100
```

###Find your recently viewed items

```
MATCH (u :`User`)-[r :VIEWED]->(i :`Item`)
WHERE u.id = {id}
RETURN i.id AS id, i.description AS description, r.dt_added AS timestamp
ORDER BY COALESCE (r.dt_added, -5000) DESC
LIMIT {limit}
```

###Items bought after viewing item

```
MATCH (u :`User`)-[view_rel :VIEWED]->(i  :`Item`)
WHERE u.id = 53001
WITH u,view_rel,i
MATCH (u)-[buy_rel  :`BUY`]->(i2  :`Item`)
WHERE view_rel.sessionid = buy_rel.sessionid AND view_rel.dt_added < buy_rel.dt_added
RETURN DISTINCT buy_rel.sessionid AS sessionid, COLLECT(i.id) AS viewedItem, COLLECT(i2.id) AS boughtItems
```

Final query (finds most recent view-then-bought relationships, and cap the number of results):

```
MATCH (u :`User`)-[view_rel :VIEWED]->(i  :`Item`)
WHERE i.id = 7638
WITH u,view_rel,i
MATCH (u)-[buy_rel  :`BUY`]->(i2  :`Item`)
WHERE view_rel.sessionid = buy_rel.sessionid AND view_rel.dt_added < buy_rel.dt_added AND i <> i2
RETURN DISTINCT buy_rel.sessionid AS sessionId, buy_rel.dt_added AS timestamp, i.id AS viewedItem, COLLECT(i2.id) AS boughtItems
ORDER BY COALESCE(timestamp, -5000) DESC
LIMIT 100
```

###Find trending items

```
MATCH (u :`User`)-[view_rel :VIEWED]->(i  :`Item`)
WITH u,view_rel,i
RETURN i.id AS id, i.description AS description, view_rel.dt_added AS timestamp
ORDER BY COALESCE(timestamp, -5000) DESC
LIMIT 800
```

###OI[V/P]T
####Legacy Query

On the same occasion an item was viewed or bought, what other items were viewed or bought along with it?

```
MATCH (i :`Item` :%s)<-[r  :`BUY`]-(u  :`User`)
WITH r.sessionid AS sessionId, COLLECT(i.id) AS items, COUNT(i.id) AS basketSize
WHERE ANY(item in items WHERE item IN {ids}) AND basketSize > 1
RETURN sessionId, items, basketSize
LIMIT {limit}
```


####Optimised Query
```
MATCH (s :`Session`)-[r1  :`BUY`]->(i  :`Item`  :`Store` {id : {itemId}})
MATCH (s)-[r2  :`BUY`]->(x  :`Item`  :`Store`)
WHERE x <> i
RETURN x AS item, COUNT(x)/{limit} AS score
LIMIT 100
```

With sessions grouped for each user

```
######################## MARKED
MATCH (i  :`Item`  :`Store` {id : 5550})<-[r1  :`BUY`]-(u  :`User`)
MATCH (x  :`Item`  :`Store`)<-[r2  :`BUY`]-(u)
WHERE r1.sessionid = r2.sessionid AND x <> i
RETURN u.id AS userId, COLLECT(DISTINCT r1.sessionid) AS sessions, COLLECT(DISTINCT x.id) AS items, COUNT(x.id) AS basketSize
LIMIT 100
```


###ActionType-1 followed by ActionType-2 on the Same Session
```
MATCH (u :`User`  :`Store`)-[first_rel  :`BUY`]->(i  :`Item`  :`Store` {id:12651})
WITH u,first_rel,i
MATCH (u)-[sec_rel :VIEWED]->(i2  :`Item`  :`Store`)
WHERE first_rel.sessionid = sec_rel.sessionid AND first_rel.dt_added < sec_rel.dt_added AND i <> i2
RETURN i.id AS collectionId, COLLECT(DISTINCT sec_rel.sessionid) AS collections, COLLECT(i2.id) AS items
ORDER BY COALESCE(collectionId, -5000) DESC
```

###Viewed if purchased

```
MATCH (u :`User`  :`Store`)-[first_rel :VIEWED]->(i  :`Item`  :`Store` {id:{itemId}})
WITH u,first_rel,i
MATCH (u)-[sec_rel  :`BUY`]->(i2  :`Item`  :`Store`)
WHERE i <> i2
RETURN i.id AS collectionId, COLLECT(DISTINCT sec_rel.sessionid) AS collectios, COLLECT(i2.id) AS items
ORDER BY COALESCE(collectionId, -5000) DESC
LIMIT 100
```

###OIV/OPI

What else was viewed or purchased by the same person or from the same agent on a different occasion, i.e. different session?

####Legacy OIV/OIP

```
MATCH (i :`Store` :`Item` {id:{item_id}})<-[r :`REL`]-(s :`Store` :`Session`)-[:`REL`]->(x :`Store` :`Item`)
WHERE i <> x
RETURN DISTINCT x.id AS id, COUNT(x.id) AS matches
ORDER BY matches DESC
LIMIT {limit}
```

####Optimised OIV/OIP

```
MATCH (i  :`Store` :`Item` {id:5132})
WITH i
MATCH (i)<-[r :`REL`]-(s  :`Store` :`Session`)
WITH DISTINCT s, i, r
ORDER BY r.timestamp DESC
LIMIT 200
MATCH (s)-[ :`REL`]->(x  :`Store` :`Item`)
WHERE i <> x
RETURN DISTINCT x.id AS id, COUNT(x.id) AS matches
ORDER BY matches DESC
LIMIT 5
```

####Shorthand

Form:
```
MATCH (i :`Store` :`Item` {id:{item_id}})<-[r1 :`REL`]-(s1 :`Store` :`Session`)-[:`BY`]->(u :`Store` :`User`)<-[:`BY`]-(s2 :`Store` :`Session`)-[:`REL`]->(x :`Store` :`Item`)
WHERE i <> x AND s1 <> s2
RETURN x.id AS id
LIMIT {limit}
```

Example:
```
MATCH (i  :`Store` :`Item` {id:5132})<-[r1 :`BUY`]-(s1  :`Store`:session)-[:`BY`]->(u  :`Store` :`User`/`Agent`)<-[:`BY`]-(s2  :`Store`:session)-[:`BUY`]->(x  :`Store` :`User`)
WHERE i <> x AND s1 <> s2
RETURN x.id AS id
LIMIT 300
```

Note: Agent is used when there is little user data, i.e. anonymous browsers.


###User: most recently purchase/viewed/bought (for users with many transactions)

Items: 33439; 32766

####Legacy Query
```
MATCH (u :`Store` :`User` {id:33439})<-[:`BY`]-( :`Store`:session)-[r :`BUY`]->(x :`Store` :`Item`)
WITH DISTINCT r, x
ORDER BY r.timestamp DESC
LIMIT 50
RETURN DISTINCT x.id AS id, COUNT(x.id) AS matches
ORDER BY matches DESC
LIMIT 5
```

####Optimised Query

Form:
```
MATCH (u :`Store` :`User` {id:{item_id}})
WITH u
MATCH (u)<-[:`BY`]-(s :`Store` :`Session`)
WITH DISTINCT s
MATCH (s)-[r :`REL`]->(x :`Store` :`Session`)
WITH r, x
ORDER BY r.timestamp DESC
LIMIT {ntx}
RETURN DISTINCT x.id AS id, COUNT(x) AS matches
ORDER BY matches DESC
LIMIT {limit}
```

Example:
```
MATCH (u :`User` :`Store` {id:33439})
WITH u
MATCH (u)<-[:`BY`]-(s :`Store` :`Session`)
WITH DISTINCT s
MATCH (s)-[r :`BUY`]->(x :`Store` :`Item`)
WITH r, x
ORDER BY r.timestamp DESC
LIMIT 50
RETURN DISTINCT x.id AS id, COUNT(x) AS matches
ORDER BY matches DESC
LIMIT 5
```


###Out of the last 50 viewed/added to cart items, which ones have never been purchased by the user?

```
MATCH (u  :`Store` :`User` {id:33439})
WITH u
MATCH (u)<-[:`BY`]-(s  :`Store` :`Session`)
WITH DISTINCT s
MATCH (s)-[vr :`VIEW`]->(x  :`Store` :`Item`)
WITH vr, x, s
ORDER BY vr.timestamp DESC
LIMIT 50
OPTIONAL MATCH (s)-[br :`BUY`]->(x)
WHERE br is NULL
RETURN DISTINCT x.id AS id, COUNT(x) AS matches
ORDER BY matches DESC
LIMIT 5
```

```
MATCH (u  :`Store` :`User` {id:33439})
WITH u
MATCH (u)<-[:`BY`]-(s  :`Store` :`Session`)
WITH DISTINCT s
MATCH (s)-[vr :`VIEW`]->(x  :`Store` :`Item`)
WITH vr, x, s
ORDER BY vr.timestamp DESC
LIMIT 50
OPTIONAL MATCH (s)-[br :`BUY`]->(x)
WHERE br is NULL
RETURN DISTINCT x.id AS id, COUNT(x) AS matches
ORDER BY matches DESC
LIMIT 5
```
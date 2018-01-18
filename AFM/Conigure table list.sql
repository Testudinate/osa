

item/store(tall table) tables will sync from silos periodically daily.
我们每天定期从silo上面去抽取product/store纵表, 然后在OSA数据库里面把standard和OSM的attributes做到olap_item表里面去。
至于customized的attributes需要调用对应的service去拿到相关的DataFrame。
这样就解决了item/store的问题

All configure tables will be stored in centralized MSSql DB.
configure table list:
rsi_dim_silos



<#macro type_info_bdb b>
<#global bdb = b.name>
<#global Bdb = b.name?cap_first>
</#macro> 

<!-- Lock mode for BDB LockMode.IGNORE_LEASES-->
<#macro lock>
<#if bdb = "db">
LockMode.READ_UNCOMMITTED 
<#else>
LockMode.READ_UNCOMMITTED 
</#if>
</#macro> 

<!-- Hash table mode for BDB -->
<#global bdb_mode = "BTREE">

<#macro openDB>
<#if bdb = "db">
env.openDatabase(null, name, name, dbConfig)
<#else>
env.openDatabase(null, name, dbConfig)
</#if>
</#macro> 

<#macro bdbStats>
			<#if bdb_mode = "HASH">
			HashStats
			<#else>
			BtreeStats
			</#if>
</#macro>

<#macro openDBSeq>
<#if bdb = "db">
env.openDatabase(null,  sequentialDatabaseName(name),  sequentialDatabaseName(name), dbConfig)
<#else>
env.openDatabase(null,  sequentialDatabaseName(name), dbConfig)
</#if>
</#macro> 

<#macro prepareStorageDevice>
DatabaseConfig dbConfig = createDefaultDatabaseConfig();
				boolean temp = config.isTemp();
				boolean bulkMode = config.isBulkMode();
				boolean duplicates = config.isDuplicates();							 
								
								// bulk mode has priority over deferred write.
								<#if bdb = "je">
										 dbConfig.setSortedDuplicates(duplicates);
								if(bulkMode){
										dbConfig.setDeferredWrite(bulkMode);										
								}else{
										dbConfig.setTemporary(temp);
								}
								
								<#else>
//										 dbConfig.setUnsortedDuplicates(duplicates);	
										 dbConfig.setSortedDuplicates(duplicates);									 
										 dbConfig.setNoMMap(false);
								</#if>
											Database seq = null;
						if(!duplicates){
								seq = <@openDBSeq/>;
						}
</#macro>
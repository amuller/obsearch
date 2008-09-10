<#macro type_info_bdb b>
<#global bdb = b.name>
<#global Bdb = b.name?cap_first>
</#macro> 

<!-- Lock mode for BDB LockMode.IGNORE_LEASES-->
<#macro lock>
<#if bdb = "db">
null
<#else>
null
</#if>
</#macro> 



<#macro openDB>
<#if bdb = "db">
env.openDatabase(null, name, name, dbConfig)
<#else>
env.openDatabase(null, name, dbConfig)
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
								</#if>
											Database seq = null;
						if(!duplicates){
								seq = <@openDBSeq/>;
						}
</#macro>
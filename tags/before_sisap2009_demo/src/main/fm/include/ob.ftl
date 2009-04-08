<#-- General variables used by all the templates  -->


<#-- generation of inverted results -->
<#assign results = ["Inverted", ""]>

<#-- Header used on top of each file -->
<#macro type_info t>
<#global type = t.name>
<#global Type = t.name?cap_first>
<#-- used for doing the typical Integer.MAX_VALUE -->
<#if t.name == "int">
<#global ClassType = "Integer">
<#else>
<#global ClassType = Type>
</#if>

<#-- used for accessing ByteBuffer objects -->
<#if t.name == "byte">
<#global BBType = "">
<#else>
<#global BBType = Type>
</#if>
</#macro> 

<#-- Binding exceptions used in BDB -->
<#macro binding_info t>
<#-- Call the previous template -->
<#if t.name == "int">
<#global binding = "Integer">
<#assign binding2 = "Int">
<#elseif t.name == "float">
<#global binding = "SortedFloat">
<#assign binding2 = binding>
<#elseif t.name == "double">
<#global binding = "SortedDouble">
<#assign binding2 = binding>
<#else>
<#global binding = Type>
<#assign binding2 = t.name?cap_first>
</#if>
</#macro> 


<#macro gen_warning filename>
//*************************************************************************
//****** Warning: this is a generated file ********************************
//****** The source file is: ${filename}   
//*************************************************************************
</#macro>

<#macro gen_simple_warning>
//************************************************
//****** Warning: this is a generated file *******
//************************************************
</#macro>

<#-- Generate a random value for the given type 
     The random  object must be called "r"
-->


<#-- Generate a random value for the given type 
     on the random object r with the given 
     dimensionality.
-->
<#macro random r dimensionality>
  			
  <#if type == "int" || type == "short" || type == "byte">
	 (${type})${r}.nextInt(${ClassType}.MAX_VALUE/${dimensionality});
	<#elseif type == "float" || type == "double"/>
	  ${r}.next${Type}();
  <#else>
	 ${r}.next${Type}()/${dimensionality};
  </#if>
</#macro>

<#-- vector size depending on the type used -->
<#macro vectorSize>
<#if type == "byte">
15
<#else>
200
</#if>
</#macro>

<#macro min_value>
<#if type == "double" || type == "float">
${ClassType}.NEGATIVE_INFINITY 
<#else>
${ClassType}.MIN_VALUE
</#if>
</#macro>


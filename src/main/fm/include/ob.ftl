<#-- General variables used by all the templates  -->

<#-- Header used on top of each file -->
<#macro type_info t>
<#global type = t.name>
<#global Type = t.name?cap_first>
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

<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#include "/@inc/index.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="Per${Type}.java" />
package net.obsearch.index.perm.impl;

public class Per${Type} implements Comparable<Per${Type}>{
		private ${type} distance;
		private short id;
		public Per${Type}(${type} distance, short id) {
			super();
			this.distance = distance;
			this.id = id;
		}
		@Override
		public int compareTo(Per${Type} o) {
			if(distance < o.distance){
				return -1;
			}else if(distance > o.distance){
				return 1;
			}else{
				if(id < o.id){
					return -1;
				}else if(id > o.id){
					return 1;
				}else{
					return 0;
				}
			}
		}

		public boolean equals(Object o){
				Per${Type} ot = (Per${Type})o;
				return  id == ot.id;
		}
		
		
		public int hashCode(){
				return id;
		}
		
		public ${type} getDistance() {
			return distance;
		}
		public void setDistance(${type} distance) {
			this.distance = distance;
		}
		public short getId() {
			return id;
		}
		public void setId(short id) {
			this.id = id;
		}
		
		
		
	}


</#list>
<#-- To render the third-party file.
 URL: https://gist.github.com/matthesrieke/8819894
 Available context :
 - dependencyMap a collection of Map.Entry with
   key are dependencies (as a MavenProject) (from the maven project)
   values are licenses of each dependency (array of string)
 - licenseMap a collection of Map.Entry with
   key are licenses of each dependency (array of string)
   values are all dependencies using this license
-->
<#function licenseFormat licenses>
	<#assign result><#list licenses as license>[${license}]<#if (license_has_next)> - </#if></#list></#assign>
	<#assign result = "\"" + result + "\","/>
    <#return result>
</#function>
<#function artifactFormat p>
    <#if p.name?index_of('Unnamed') &gt; -1>
        <#return "\"" + p.artifactId + "\",\"" + p.groupId + ":" + p.artifactId + ":" + p.version + "\",\"" + (p.url!"no url defined") + "\"">
    <#else>
        <#return "\"" + p.name + "\",\"" + p.groupId + ":" + p.artifactId + ":" + p.version + "\",\"" + (p.url!"no url defined") + "\"">
    </#if>
</#function>
<#if dependencyMap?size == 0>
The project has no dependencies.
<#else>
"License","Name","Artifact","URL"
    <#list dependencyMap as e>
        <#assign project = e.getKey()/>
        <#assign licenses = e.getValue()/>
${licenseFormat(licenses)}${artifactFormat(project)}
    </#list>
</#if> 


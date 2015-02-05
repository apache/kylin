package org.apache.kylin.bootstrap;

import java.net.URL;

import org.apache.catalina.core.StandardContext;
import org.apache.catalina.deploy.ErrorPage;
import org.apache.catalina.startup.Constants;
import org.apache.catalina.startup.ContextConfig;
import org.apache.catalina.startup.DigesterFactory;
import org.apache.catalina.startup.WebRuleSet;
import org.apache.catalina.util.SchemaResolver;
import org.apache.tomcat.util.digester.Digester;
import org.apache.tomcat.util.digester.RuleSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class overrides ContextConfig (unfortunately requiring a bunch of cut-n-paste)
 * in order to remove registering JSP/TLD namespaces which cause a ton of warnings on startup
 */
public class MinimalContextConfig extends ContextConfig {
	private static final Logger log = LoggerFactory.getLogger(MinimalContextConfig.class);
	private final StandardContext cx;

	MinimalContextConfig(StandardContext cx) {
		this.cx = cx;
	}

	@Override
	protected synchronized void configureStart() {
		super.configureStart();

		// clear out all default error content
		for (ErrorPage page : cx.findErrorPages()) {
			cx.removeErrorPage(page);
		}
	}

	/**
	 * Taken from org.apache.catalina.startup.ContextConfig
	 * ====================================================
	 * Create and return a Digester configured to process the
	 * web application deployment descriptor (web.xml).
	 */
	@Override
	public void createWebXmlDigester(boolean namespaceAware, boolean validation) {

		webRuleSet = new WebRuleSet(false);
		webDigester = newDigester(validation, namespaceAware, webRuleSet);
		webDigester.getParser();

		webFragmentRuleSet = new WebRuleSet(true);
		webFragmentDigester = newDigester(validation, namespaceAware, webFragmentRuleSet);
		webFragmentDigester.getParser();
	}

	/**
	 * Taken from org.apache.catalina.startup.DigesterFactory
	 * ======================================================
	 * Create a <code>Digester</code> parser.
	 * @param xmlValidation turn on/off xml validation
	 * @param xmlNamespaceAware turn on/off namespace validation
	 * @param rule an instance of <code>RuleSet</code> used for parsing the xml.
	 */
	protected static Digester newDigester(boolean xmlValidation,
								boolean xmlNamespaceAware,
								RuleSet rule) {
		Digester digester = new Digester();
		digester.setNamespaceAware(xmlNamespaceAware);
		digester.setValidating(xmlValidation);
		digester.setUseContextClassLoader(true);

		SchemaResolver schemaResolver = new SchemaResolver(digester);
		registerLocalSchema(schemaResolver);
		
		digester.setEntityResolver(schemaResolver);
		if ( rule != null ) {
			digester.addRuleSet(rule);
		}

		return (digester);
	}

	/**
	 * Taken from org.apache.catalina.startup.DigesterFactory
	 * ======================================================
	 * Utilities used to force the parser to use local schema, when available,
	 * instead of the <code>schemaLocation</code> XML element.
	 */
	protected static void registerLocalSchema(SchemaResolver schemaResolver){
		// J2EE
		register(Constants.J2eeSchemaResourcePath_14,
				 Constants.J2eeSchemaPublicId_14,
				 schemaResolver);

		register(Constants.JavaeeSchemaResourcePath_5,
				Constants.JavaeeSchemaPublicId_5,
				schemaResolver);

		register(Constants.JavaeeSchemaResourcePath_6,
				Constants.JavaeeSchemaPublicId_6,
				schemaResolver);

		// W3C
		register(Constants.W3cSchemaResourcePath_10,
				 Constants.W3cSchemaPublicId_10,
				 schemaResolver);

		register(Constants.W3cSchemaDTDResourcePath_10,
				Constants.W3cSchemaDTDPublicId_10,
				schemaResolver);

		register(Constants.W3cDatatypesDTDResourcePath_10,
				Constants.W3cDatatypesDTDPublicId_10,
				schemaResolver);

		// web.xml	  
		register(Constants.WebDtdResourcePath_22,
				 Constants.WebDtdPublicId_22,
				 schemaResolver);

		register(Constants.WebDtdResourcePath_23,
				 Constants.WebDtdPublicId_23,
				 schemaResolver);

		register(Constants.WebSchemaResourcePath_24,
				 Constants.WebSchemaPublicId_24,
				 schemaResolver);

		register(Constants.WebSchemaResourcePath_25,
				Constants.WebSchemaPublicId_25,
				schemaResolver);

		register(Constants.WebSchemaResourcePath_30,
				Constants.WebSchemaPublicId_30,
				schemaResolver);

		register(Constants.WebCommonSchemaResourcePath_30,
				Constants.WebCommonSchemaPublicId_30,
				schemaResolver);
		
		register(Constants.WebFragmentSchemaResourcePath_30,
				Constants.WebFragmentSchemaPublicId_30,
				schemaResolver);

		// Web Service
		register(Constants.J2eeWebServiceSchemaResourcePath_11,
				 Constants.J2eeWebServiceSchemaPublicId_11,
				 schemaResolver);

		register(Constants.J2eeWebServiceClientSchemaResourcePath_11,
				 Constants.J2eeWebServiceClientSchemaPublicId_11,
				 schemaResolver);

		register(Constants.JavaeeWebServiceSchemaResourcePath_12,
				Constants.JavaeeWebServiceSchemaPublicId_12,
				schemaResolver);

		register(Constants.JavaeeWebServiceClientSchemaResourcePath_12,
				Constants.JavaeeWebServiceClientSchemaPublicId_12,
				schemaResolver);

		register(Constants.JavaeeWebServiceSchemaResourcePath_13,
				Constants.JavaeeWebServiceSchemaPublicId_13,
				schemaResolver);

		register(Constants.JavaeeWebServiceClientSchemaResourcePath_13,
				Constants.JavaeeWebServiceClientSchemaPublicId_13,
				schemaResolver);
	}

	/**
	 * Taken from org.apache.catalina.startup.DigesterFactory
	 * ======================================================
	 * Load the resource and add it to the resolver.
	 */
	protected static void register(String resourceURL, String resourcePublicId,
			SchemaResolver schemaResolver){
		URL url = DigesterFactory.class.getResource(resourceURL);
   
		if (url == null) {
			log.warn("Could not get URL [%s]", resourceURL);

		} else {
			schemaResolver.register(resourcePublicId , url.toString() );
		}
	}
}
import org.apache.nifi.expression.ExpressionLanguageScope
import xyz.capybara.clamav.ClamavClient
import xyz.capybara.clamav.commands.scan.result.ScanResult
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.annotation.lifecycle.OnStopped

import java.io.InputStream

class ClamAvProcessor implements Processor {
    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .displayName("Host")
            .name("clam-host")
            .description("The hostname for the ClamAV daemon.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
	    .build()
    
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .displayName("Port")
            .name("clam-port")
            .description("The port for the ClamAV daemon.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
	    .build()

  
    def REL_SUCCESS = new Relationship.Builder().name("success").description('FlowFiles that pass a ClamAV scan are routed here').build()
    def REL_SCANFAILURE = new Relationship.Builder().name("scanfailure").description('FlowFiles that do not pass a ClamAV scan are routed here').build()    
    def REL_FAILURE = new Relationship.Builder().name("failure").description('FlowFiles that fail to process are routed here').build()    

    def log
    def clamHost = null
    def clamPort = -1
    ClamavClient client = null;
    private Set<Relationship> relationships;
    
    private List<PropertyDescriptor> properties;    
    
    @Override    
    Set<Relationship> getRelationships() {
        return relationships;
    }    

    @Override
    public void initialize(final ProcessorInitializationContext context) {        
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HOST);
        properties.add(PORT);
        this.properties = Collections.unmodifiableList(properties);        
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_SCANFAILURE);
        rels.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(rels);
    }    
    
    
    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        return properties;
    }
   
    @Override
    public PropertyDescriptor getPropertyDescriptor(String descriptorName) {
	final PropertyDescriptor specDescriptor = new PropertyDescriptor.Builder().name(descriptorName).build();
        final List<PropertyDescriptor> propertyDescriptors = getPropertyDescriptors();
        if (propertyDescriptors != null) {
            for (final PropertyDescriptor desc : propertyDescriptors) { //find actual descriptor
                if (specDescriptor.equals(desc)) {
                    return desc;
                }
            }
        }
	return null;


    }
 
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
	def session = sessionFactory.createSession()
        def flowFile = session.get()
	if (client == null || clamHost == null || clamPort == null) {
	
        	clamHost = context.getProperty(HOST).evaluateAttributeExpressions().getValue()
        	clamPort = context.getProperty(PORT).evaluateAttributeExpressions().asInteger()
        
        	client = new ClamavClient(clamHost, clamPort);

	}
        try {            
            if (!flowFile) 
                return
       
            final FlowFile original = flowFile
            
            ScanResult scanResult;
            session.read(flowFile,
                    { inputStream ->
                    scanResult = client.scan(inputStream);
                } as InputStreamCallback)
            
            if (scanResult instanceof ScanResult.OK) {
                flowFile = session.putAttribute(flowFile, "clamav", "OK");
                session.transfer(flowFile, REL_SUCCESS)
                session.commit()
            } else if (scanResult instanceof ScanResult.VirusFound) {
                Map<String, Collection<String>> results = ((ScanResult.VirusFound) scanResult).getFoundViruses();
                flowFile = session.putAttribute(flowFile, "clamav", "Fail");
                flowFile = session.putAttribute(flowFile, "clamav.results", results.get("stream").toString());
                session.transfer(flowFile, REL_SCANFAILURE)
                session.commit()
            } else {
                session.transfer(flowFile,REL_FAILURE)
                session.commit()
            }

        }
        catch (e) {
	    session.transfer(flowFile,REL_FAILURE)
            session.commit()
            throw new ProcessException(e)        
        }
    }    
      
    

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
    	clamHost = null;
    	clamPort = -1;
	client = null;
    }
    @Override
    public Collection<ValidationResult> validate(final ValidationContext context) {
        return Collections.emptySet();
    }
        
    @Override
    String getIdentifier() { return null }
}
processor = new ClamAvProcessor()

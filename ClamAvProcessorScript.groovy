import org.apache.nifi.expression.ExpressionLanguageScope
import xyz.capybara.clamav.ClamavClient
import xyz.capybara.clamav.commands.scan.result.ScanResult
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.annotation.lifecycle.OnScheduled
import org.apache.nifi.annotation.lifecycle.OnStopped

import java.io.InputStream

class ClamAvProcessor extends AbstractProcessor {
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
    
    ClamavClient client
    
    
    @Override    
    Set<Relationship> getRelationships() {
        return [REL_SUCCESS,REL_SCANFAILURE,REL_FAILURE] as Set
    }    
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        def flowFile = session.get()
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
    List<PropertyDescriptor> getSupportedPropertyDescriptors() { 
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HOST);  
        descriptors.add(PORT);
	return descriptors;
    }    

    
    @OnScheduled
    public final void onScheduled(final ProcessContext context) throws IOException {
        def clamHost = context.getProperty(HOST).evaluateAttributeExpressions().getValue()
        def clamPort = context.getProperty(PORT).evaluateAttributeExpressions().asInteger()
        
        client = new ClamavClient(clamHost, clamPort);
    }
    
}
processor = new ClamAvProcessor()

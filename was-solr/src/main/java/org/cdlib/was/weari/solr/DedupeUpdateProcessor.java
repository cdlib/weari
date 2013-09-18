package org.cdlib.was.weari.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.BinaryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.client.solrj.response.QueryResponse;

class DedupeUpdateProcessor extends UpdateRequestProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DedupeUpdateProcessor.class);

    private static final String REQUEST_HANDLER = "/select";
    private static final String ID_FIELD = "id";
    private static final String URL_FIELD = "url";
    private static final String CONTENT_FIELD = "content";
    private static final String SHA1_FIELD = "sha1";
    
    /* multivalued fields */
    private static final String JOB_FIELD = "job";
    private static final String DATE_FIELD = "date";
    private static final String ARCNAME_FIELD = "arcname";
    private static final String[] UPDATE_FIELD_ARR = { JOB_FIELD, DATE_FIELD, ARCNAME_FIELD };

    private static final Map REQ_ARGS = new HashMap(){{
        put("rows", new String[]{ "1" });
        put("fl", new String[]{ DATE_FIELD, JOB_FIELD, ARCNAME_FIELD });
    }};

    public DedupeUpdateProcessor(UpdateRequestProcessor next) {
        super(next);
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
        SolrInputDocument updateDoc = cmd.getSolrInputDocument();
        String id = (String) updateDoc.getField(ID_FIELD).getValue();
        String query = "+" + ID_FIELD + ":" + escape(id);
        final SolrIndexSearcher searcher = cmd.getReq().getSearcher();
        LocalSolrQueryRequest newReq = new LocalSolrQueryRequest(cmd.getReq().getCore(), query, "", 0, 1, REQ_ARGS) {
                @Override
                public SolrIndexSearcher getSearcher() { return searcher; }
                @Override
                public void close() { }
            };
        SolrQueryResponse rsp = new SolrQueryResponse();
        cmd.getReq().getCore().execute(cmd.getReq().getCore().getRequestHandler(REQUEST_HANDLER), newReq, rsp);
        QueryResponse qRsp = new QueryResponse();
        qRsp.setResponse(BinaryResponseWriter.getParsedResponse(newReq, rsp));
        if (qRsp.getResults().size() > 0) {
            SolrDocument existingDoc = qRsp.getResults().get(0);
                
            Collection existingArcnames = existingDoc.getFieldValues(ARCNAME_FIELD);

            if (existingArcnames.contains((String)updateDoc.getFieldValue(ARCNAME_FIELD))) {
                /* abort if already indexed */
                return;
            } else {
                List<String> removedFields = new ArrayList<String>();
                /* fields that should be the same */
                for (String fieldName : updateDoc.getFieldNames()) {
                    if (!fieldName.equals(DATE_FIELD) &&
                        !fieldName.equals(JOB_FIELD) &&
                        !fieldName.equals(ARCNAME_FIELD) &&
                        !fieldName.equals(ID_FIELD) &&
                        !fieldName.equals(CONTENT_FIELD)) {
                        removedFields.add(fieldName);
                    }
                }

                for (String removedField : removedFields) {
                    updateDoc.removeField(removedField);
                }

                for (String f : UPDATE_FIELD_ARR) {
                    final String val = (String) updateDoc.getFieldValue(f);
                    updateDoc.setField(f, new HashMap(){{ put("add", val); }});
                }

                /* in case we had empty content */
                String oldContent = (String) existingDoc.getFieldValue(CONTENT_FIELD);
                if (oldContent == null || oldContent == "") {
                    final String updateDocContent = (String) updateDoc.getFieldValue(CONTENT_FIELD);
                    updateDoc.setField(CONTENT_FIELD, new HashMap(){{ put("set", updateDocContent); }});
                }
            }
        }
        super.processAdd(cmd);
    }            

    private String escape(String orig) {
        return orig.replace(":", "\\:");
    }
}

package cloudcomputing.resource;

import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Represents a Base SQSD Resource
 */
public abstract class AbstractSQSDResource extends BaseResource {
    @Consumes(MediaType.APPLICATION_JSON)
    @POST
    public Response execute(@Context HttpHeaders headers, String bodyNode) throws Exception {
        return executeInternal(headers, bodyNode);
    }

    protected abstract Response executeInternal(HttpHeaders headers, String bodyNode);
}

package com.database.atypon.Node.controllers.read;

import com.database.atypon.Node.security.SecurityUtils;
import com.database.atypon.Node.services.read.QuorumReadService;
import com.database.atypon.Node.services.read.ReadService;
import com.database.atypon.Node.utils.Forwarder;
import com.database.atypon.Node.utils.response.Response;
import com.database.atypon.Node.utils.response.ResponseType;
import com.database.atypon.Node.utils.ring.RingRouter;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/user/read")
public class ReadController {

   private final ReadService readService;
   private final QuorumReadService quorumReadService;

   public ReadController(ReadService readService, QuorumReadService quorumReadService) {
       this.readService = readService;
       this.quorumReadService = quorumReadService;
   }

   private static boolean isInternal() {
       return "INTERNAL".equals(SecurityUtils.currentRole());
   }

   @RequestMapping("/document")
    public Response fetchById(@RequestParam String id, @RequestParam String databaseName,
                              @RequestParam String schemaName) {
       //validate input parameters
       if(id == null || id.isEmpty())
           return new Response(ResponseType.ERROR, "Id is null or empty");
       if(databaseName == null || databaseName.isEmpty())
           return new Response(ResponseType.ERROR, "Database name is null or empty");
       if(schemaName == null || schemaName.isEmpty())
           return new Response(ResponseType.ERROR, "Schema name is null or empty");

       // a peer coordinating a quorum read asks for this node's own copy only
       if (isInternal())
           return readService.fetchById(id, databaseName, schemaName);

       // a node that holds no copy of the schema hands the read to one that does
       if (!RingRouter.isReplica(databaseName, schemaName))
           return Forwarder.forwardForResponse(RingRouter.primaryFor(databaseName, schemaName),
                   "/user/read/document?id=" + id + "&databaseName=" + databaseName + "&schemaName=" + schemaName,
                   null, false);

       // a client read asks every replica and answers with the freshest copy, repairing stale ones
       return quorumReadService.readById(databaseName, schemaName, id);
   }

    @RequestMapping("/all")
     public Response fetchAll(@RequestParam String databaseName,
                              @RequestParam String schemaName) {
       if(databaseName == null || databaseName.isEmpty())
           return new Response(ResponseType.ERROR, "Database name is null or empty");
       if(schemaName == null || schemaName.isEmpty())
          return new Response(ResponseType.ERROR, "Schema name is null or empty");

       // a node that holds no copy of the schema hands the read to one that does
       if (!RingRouter.isReplica(databaseName, schemaName))
           return Forwarder.forwardForResponse(RingRouter.primaryFor(databaseName, schemaName),
                   "/user/read/all?databaseName=" + databaseName + "&schemaName=" + schemaName, null, false);

       return readService.fetchAll(databaseName, schemaName);
    }
}

package com.database.atypon.Node.controllers.read;

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

   public ReadController(ReadService readService) {
       this.readService = readService;
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

       // a node that holds no copy of the schema hands the read to one that does
       if (!RingRouter.isReplica(databaseName, schemaName))
           return Forwarder.forwardForResponse(RingRouter.primaryFor(databaseName, schemaName),
                   "/user/read/document?id=" + id + "&databaseName=" + databaseName + "&schemaName=" + schemaName,
                   null, false);

       return readService.fetchById(id, databaseName, schemaName);
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

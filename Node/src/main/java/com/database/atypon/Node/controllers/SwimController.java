package com.database.atypon.Node.controllers;

import com.database.atypon.Node.model.Node;
import com.database.atypon.Node.utils.swim.SwimClient;
import com.database.atypon.Node.utils.swim.dto.AckMessage;
import com.database.atypon.Node.utils.swim.dto.PingMessage;
import com.database.atypon.Node.utils.swim.dto.PingReqMessage;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * The receiving end of SWIM failure detection. Node-to-node only: {@code /swim/**} requires the
 * INTERNAL role (see {@code SecurityConfig}).
 */
@RestController
public class SwimController {

    private final SwimClient client;

    public SwimController(SwimClient client) {
        this.client = client;
    }

    /** A direct probe. Being able to answer at all is the ack; there is no separate ack endpoint. */
    @PostMapping("/swim/ping")
    public AckMessage ping(@RequestBody(required = false) PingMessage ping) {
        return new AckMessage(true);
    }

    /**
     * An indirect probe: pings the named target on the caller's behalf and reports whether it acked.
     * The caller could not reach the target itself; this node may be able to.
     */
    @PostMapping("/swim/ping-req")
    public AckMessage pingReq(@RequestBody PingReqMessage request) {
        Node target = request.getTarget();
        if (target == null || target.getName() == null || target.getPort() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ping-req needs a target with a name and a port");
        }
        return new AckMessage(client.ping(target));
    }
}

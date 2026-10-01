import { DurableObject } from "cloudflare:workers";

const MAX_USERNAME_LENGTH = 40;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";

    // =====================================================
    // TEST
    // =====================================================

    if (path === "/" || path === "/health") {
      return json({
        success: true,
        service: "Weizend Realtime",
        realtime: "websocket-hibernation-v1"
      });
    }


    // =====================================================
    // MARKET -> WEBSOCKET BAĞLANTISI
    // =====================================================

    if (path === "/ws") {
      if (request.method !== "GET") {
        return json({
          success: false,
          error: "GET gerekli."
        }, 405);
      }

      const upgrade =
        request.headers.get("Upgrade") || "";

      if (upgrade.toLowerCase() !== "websocket") {
        return json({
          success: false,
          error: "WebSocket bağlantısı gerekli."
        }, 426);
      }

      const username =
        cleanUsername(
          url.searchParams.get("user")
        );

      if (!isValidUsername(username)) {
        return json({
          success: false,
          error: "Geçerli kullanıcı adı gerekli."
        }, 400);
      }

      // Her Kick kullanıcısının kendine ait
      // ayrı Durable Object odası olur.
      const hub =
        env.USER_HUB.getByName(
          normalize(username)
        );

      return hub.fetch(request);
    }


    // =====================================================
    // ANA WORKER -> REALTIME BİLDİRİM
    //
    // Bu endpoint tarayıcı tarafından kullanılmayacak.
    // weizend-botrix Worker buraya bildirim gönderecek.
    // =====================================================

    if (path === "/notify") {
      if (request.method !== "POST") {
        return json({
          success: false,
          error: "POST gerekli."
        }, 405);
      }

      const expectedSecret =
        String(
          env.REALTIME_SECRET || ""
        );

      if (!expectedSecret) {
        return json({
          success: false,
          error: "REALTIME_SECRET tanımlı değil."
        }, 500);
      }

      const authorization =
        request.headers.get(
          "Authorization"
        ) || "";

      const suppliedSecret =
        authorization.startsWith("Bearer ")
          ? authorization.slice(7).trim()
          : "";

      if (
        !suppliedSecret ||
        suppliedSecret !== expectedSecret
      ) {
        return json({
          success: false,
          error: "UNAUTHORIZED"
        }, 401);
      }

      let body;

      try {
        body =
          await request.json();
      } catch {
        return json({
          success: false,
          error: "Geçerli JSON gerekli."
        }, 400);
      }

      const username =
        cleanUsername(
          body?.user
        );

      const event =
        String(
          body?.event || ""
        ).trim();

      if (!isValidUsername(username)) {
        return json({
          success: false,
          error: "Geçerli kullanıcı adı gerekli."
        }, 400);
      }

      if (
        ![
          "profile_changed",
          "auth_verified"
        ].includes(event)
      ) {
        return json({
          success: false,
          error: "Geçersiz realtime olayı."
        }, 400);
      }

      const hub =
        env.USER_HUB.getByName(
          normalize(username)
        );

      const notifyRequest =
        new Request(
          "https://internal.weizend/notify",
          {
            method: "POST",
            headers: {
              "Content-Type":
                "application/json"
            },
            body: JSON.stringify({
              event,
              username,
              data:
                body?.data ?? null,

              sentAt:
                new Date().toISOString()
            })
          }
        );

      return hub.fetch(
        notifyRequest
      );
    }


    // =====================================================
    // ENDPOINT BULUNAMADI
    // =====================================================

    return json({
      success: false,
      error: "Endpoint bulunamadı."
    }, 404);
  }
};


// =========================================================
// DURABLE OBJECT
// Her kullanıcı için bir UserHub instance'ı oluşur.
// =========================================================

export class UserHub extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);

    // "ping" mesajları Durable Object'ı uyandırmadan
    // Cloudflare tarafından "pong" ile cevaplanabilir.
    this.ctx.setWebSocketAutoResponse(
      new WebSocketRequestResponsePair(
        "ping",
        "pong"
      )
    );
  }


  async fetch(request) {
    const url =
      new URL(request.url);

    const path =
      url.pathname.replace(/\/+$/, "") || "/";


    // =====================================================
    // TARAYICI WEBSOCKET BAĞLANTISI
    // =====================================================

    if (path === "/ws") {
      const upgrade =
        request.headers.get(
          "Upgrade"
        ) || "";

      if (
        upgrade.toLowerCase() !==
        "websocket"
      ) {
        return new Response(
          "WebSocket gerekli.",
          {
            status: 426
          }
        );
      }

      const pair =
        new WebSocketPair();

      const [client, server] =
        Object.values(pair);

      // Hibernation destekli WebSocket.
      this.ctx.acceptWebSocket(
        server
      );

      server.serializeAttachment({
        connectedAt:
          new Date().toISOString(),

        connectionId:
          crypto.randomUUID()
      });

      server.send(
        JSON.stringify({
          type: "connected",
          realtime: true,
          time:
            new Date().toISOString()
        })
      );

      return new Response(
        null,
        {
          status: 101,
          webSocket: client
        }
      );
    }


    // =====================================================
    // ANA WORKER'DAN BİLDİRİM GELDİ
    // =====================================================

    if (
      path === "/notify" &&
      request.method === "POST"
    ) {
      let payload;

      try {
        payload =
          await request.json();
      } catch {
        return json({
          success: false,
          error: "Geçersiz bildirim."
        }, 400);
      }

      const sockets =
        this.ctx.getWebSockets();

      let delivered = 0;

      const message =
        JSON.stringify({
          type:
            payload.event,

          username:
            payload.username,

          data:
            payload.data ?? null,

          sentAt:
            payload.sentAt ||
            new Date().toISOString()
        });

      for (
        const socket of sockets
      ) {
        try {
          if (
            socket.readyState === 1
          ) {
            socket.send(
              message
            );

            delivered++;
          }
        } catch {
          // Kopmuş bağlantıyı atla.
        }
      }

      return json({
        success: true,
        delivered
      });
    }


    return json({
      success: false,
      error:
        "Durable Object endpoint bulunamadı."
    }, 404);
  }


  // =====================================================
  // TARAYICIDAN MESAJ GELİRSE
  // =====================================================

  async webSocketMessage(
    socket,
    message
  ) {
    // Normalde marketten mesaj beklemiyoruz.
    // ping -> pong işlemini Cloudflare otomatik yapıyor.

    if (
      typeof message === "string" &&
      message !== "ping"
    ) {
      try {
        socket.send(
          JSON.stringify({
            type: "ack"
          })
        );
      } catch {}
    }
  }


  // =====================================================
  // BAĞLANTI KAPANDI
  // =====================================================

  async webSocketClose(
    socket,
    code,
    reason
  ) {
    try {
      socket.close(
        code,
        reason
      );
    } catch {}
  }


  async webSocketError(
    socket
  ) {
    try {
      socket.close(
        1011,
        "WebSocket error"
      );
    } catch {}
  }
}


// =========================================================
// YARDIMCI FONKSİYONLAR
// =========================================================

function normalize(value) {
  return String(
    value || ""
  )
    .trim()
    .replace(/^@/, "")
    .toLowerCase();
}


function cleanUsername(value) {
  return String(
    value || ""
  )
    .trim()
    .replace(/^@/, "");
}


function isValidUsername(value) {
  const username =
    String(value || "");

  return (
    username.length >= 2 &&
    username.length <=
      MAX_USERNAME_LENGTH &&
    /^[A-Za-z0-9_]+$/.test(
      username
    )
  );
}


function json(
  data,
  status = 200
) {
  return new Response(
    JSON.stringify(data),
    {
      status,
      headers: {
        "Content-Type":
          "application/json; charset=utf-8",

        "Cache-Control":
          "no-store"
      }
    }
  );
}

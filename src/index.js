import {
  DurableObject,
  WorkerEntrypoint
} from "cloudflare:workers";


const MAX_USERNAME_LENGTH = 40;

const ALLOWED_EVENTS = [
  "profile_changed",
  "auth_verified"
];


// =========================================================
// WEIZEND REALTIME WORKER
//
// PUBLIC:
// /
// /health
// /ws?user=USERNAME
//
// INTERNAL SERVICE BINDING RPC:
// env.REALTIME.notify(username, event)
// =========================================================

export default class WeizendRealtime extends WorkerEntrypoint {

  // =======================================================
  // PUBLIC HTTP / WEBSOCKET
  // =======================================================

  async fetch(request) {

    const url =
      new URL(request.url);

    const path =
      url.pathname.replace(/\/+$/, "") || "/";


    // =====================================================
    // TEST
    // =====================================================

    if (
      path === "/" ||
      path === "/health"
    ) {

      return json({
        success: true,
        service: "Weizend Realtime",
        realtime: "websocket-hibernation-v2",
        transport: "service-binding-rpc"
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


      if (
        upgrade.toLowerCase() !==
        "websocket"
      ) {

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


      // Her Kick kullanıcısının
      // kendine ait ayrı Durable Object odası vardır.

      const hub =
        this.env.USER_HUB.getByName(
          normalize(username)
        );


      return hub.fetch(request);
    }


    // =====================================================
    // PUBLIC /notify YOK
    //
    // Bildirimler artık internet üzerinden gönderilmiyor.
    //
    // weizend-botrix:
    //
    // await env.REALTIME.notify(...)
    //
    // kullanacak.
    // =====================================================


    return json({
      success: false,
      error: "Endpoint bulunamadı."
    }, 404);

  }


  // =======================================================
  // SERVICE BINDING RPC
  //
  // ANA weizend-botrix WORKER BURAYI ÇAĞIRACAK.
  //
  // ÖRNEK:
  //
  // await env.REALTIME.notify(
  //   "Orkun",
  //   "profile_changed"
  // );
  // =======================================================

  async notify(
    usernameValue,
    eventValue
  ) {

    const username =
      cleanUsername(
        usernameValue
      );


    const event =
      String(
        eventValue || ""
      ).trim();


    if (!isValidUsername(username)) {

      return {
        success: false,
        error: "INVALID_USERNAME"
      };

    }


    if (
      !ALLOWED_EVENTS.includes(event)
    ) {

      return {
        success: false,
        error: "INVALID_EVENT"
      };

    }


    const hub =
      this.env.USER_HUB.getByName(
        normalize(username)
      );


    // Direkt ilgili kullanıcının
    // Durable Object'ına RPC çağrısı.

    return await hub.notify(
      event,
      username
    );

  }

}



// =========================================================
// USER HUB DURABLE OBJECT
//
// Her kullanıcı adı ayrı Durable Object instance'ıdır.
//
// Örnek:
//
// Orkun  -> ayrı oda
// Burak  -> ayrı oda
// Ali    -> ayrı oda
//
// !puanver @Orkun çalışırsa yalnızca
// Orkun'un odasına bildirim gider.
// =========================================================

export class UserHub extends DurableObject {

  constructor(ctx, env) {

    super(ctx, env);


    // =====================================================
    // WEBSOCKET HIBERNATION
    //
    // Bağlantı açık kalırken Durable Object
    // boşta olduğunda uyuyabilir.
    // =====================================================

    this.ctx.setWebSocketAutoResponse(

      new WebSocketRequestResponsePair(
        "ping",
        "pong"
      )

    );

  }



  // =======================================================
  // TARAYICI -> WEBSOCKET
  // =======================================================

  async fetch(request) {

    const upgrade =
      request.headers.get("Upgrade") || "";


    if (
      request.method !== "GET" ||
      upgrade.toLowerCase() !== "websocket"
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


    const [
      client,
      server
    ] =
      Object.values(pair);


    // =====================================================
    // HIBERNATABLE WEBSOCKET
    // =====================================================

    this.ctx.acceptWebSocket(
      server
    );


    // Bağlantıya küçük bir kimlik ekle.
    // Durable Object uyuyup tekrar uyansa bile
    // attachment bağlantıyla birlikte korunur.

    server.serializeAttachment({

      connectionId:
        crypto.randomUUID(),

      connectedAt:
        new Date().toISOString()

    });


    // Tarayıcıya bağlantının başarılı olduğunu bildir.

    try {

      server.send(
        JSON.stringify({
          type: "connected",
          realtime: true,
          time:
            new Date().toISOString()
        })
      );

    } catch {}


    return new Response(
      null,
      {
        status: 101,
        webSocket: client
      }
    );

  }



  // =======================================================
  // REALTIME BİLDİRİM
  //
  // Bu fonksiyon yalnızca Worker RPC tarafından çağrılır.
  // =======================================================

  async notify(
    event,
    username
  ) {

    const sockets =
      this.ctx.getWebSockets();


    // DİKKAT:
    // Burada puan / level göndermiyoruz.
    //
    // Sadece:
    //
    // "Bu kullanıcının profili değişti"
    //
    // diyoruz.
    //
    // Tarayıcı daha sonra kendi güvenli session tokenı ile
    // ana Worker'dan /profile çağıracak.
    //
    // Böylece kullanıcı verisi WebSocket üzerinden
    // açık biçimde taşınmıyor.


    const message =
      JSON.stringify({

        type:
          event,

        username:
          username,

        sentAt:
          new Date().toISOString()

      });


    let delivered = 0;


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


    return {
      success: true,
      username,
      event,
      delivered
    };

  }



  // =======================================================
  // TARAYICIDAN MESAJ GELİRSE
  // =======================================================

  async webSocketMessage(
    socket,
    message
  ) {

    // "ping" mesajına Cloudflare
    // setWebSocketAutoResponse sayesinde
    // Durable Object'ı uyandırmadan "pong" verir.


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



  // =======================================================
  // WEBSOCKET KAPANDI
  // =======================================================

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



  // =======================================================
  // WEBSOCKET HATASI
  // =======================================================

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
    String(
      value || ""
    );


  return (
    username.length >= 2 &&
    username.length <= MAX_USERNAME_LENGTH &&
    /^[A-Za-z0-9_]+$/.test(username)
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

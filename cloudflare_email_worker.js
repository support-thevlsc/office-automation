export default {
  /**
   * Email Worker handler for books@thevlsc.com
   *
   * Expected environment variables:
   * - FORWARD_TO: destination address to forward every accepted message to
   * - INTAKE_WEBHOOK_URL (optional): HTTPS endpoint to receive a JSON summary
   */
  async email(message, env, ctx) {
    const receivedAt = new Date().toISOString();
    const subject = message.headers.get("subject") || "(no subject)";
    const from = message.from || "";
    const to = message.to || "";
    const replyTo = message.headers.get("reply-to") || "";

    const summary = {
      receivedAt,
      envelope: { from, to },
      headers: { subject, replyTo },
      size: message.rawSize,
    };

    if (env.INTAKE_WEBHOOK_URL) {
      const payload = JSON.stringify(summary);
      ctx.waitUntil(
        fetch(env.INTAKE_WEBHOOK_URL, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: payload,
        }).catch((err) => {
          console.error("Failed to post intake webhook", err);
        })
      );
    }

    if (env.FORWARD_TO) {
      try {
        await message.forward(env.FORWARD_TO);
        return;
      } catch (err) {
        console.error(`Forwarding to ${env.FORWARD_TO} failed`, err);
        message.setReject(`Unable to forward inbound email: ${err}`);
        return;
      }
    }

    message.setReject("No FORWARD_TO address configured for books@thevlsc.com");
  },
};

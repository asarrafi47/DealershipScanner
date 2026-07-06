/*
 * car_narrate.js
 * Fetches an AI-generated vehicle summary for the car detail page and injects
 * it into #ai-car-description. Hides the container on any failure so the page
 * degrades gracefully. No frameworks.
 */
(function () {
  "use strict";

  function init() {
    var container = document.getElementById("ai-car-description");
    if (!container) {
      return;
    }

    var carId = container.getAttribute("data-car-id");
    if (!carId) {
      container.style.display = "none";
      return;
    }

    var body = container.querySelector(".ai-car-description__body");
    if (!body) {
      body = container;
    }

    // Loading state.
    body.textContent = "Generating summary…";

    fetch("/api/car/" + encodeURIComponent(carId) + "/narrate", {
      headers: { Accept: "application/json" },
    })
      .then(function (resp) {
        if (!resp.ok) {
          throw new Error("HTTP " + resp.status);
        }
        return resp.json();
      })
      .then(function (data) {
        if (!data || !data.ok || !data.description) {
          throw new Error(data && data.error ? data.error : "no description");
        }
        render(container, body, data.description);
      })
      .catch(function () {
        container.style.display = "none";
      });
  }

  function render(container, body, description) {
    // Clear any loading text.
    body.textContent = "";

    var label = document.createElement("div");
    label.className = "ai-car-description__label";
    label.textContent = "AI-generated summary";

    var text = document.createElement("p");
    text.className = "ai-car-description__text";
    text.textContent = description;

    body.appendChild(label);
    body.appendChild(text);
    container.style.display = "";
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

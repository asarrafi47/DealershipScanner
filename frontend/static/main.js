// Runs when the page loads
document.addEventListener("DOMContentLoaded", () => {
    console.log("JavaScript loaded");

    // Simple fade-in effect
    document.body.style.opacity = 0;

    setTimeout(() => {
        document.body.style.transition = "opacity 0.6s ease";
        document.body.style.opacity = 1;
    }, 50);
});
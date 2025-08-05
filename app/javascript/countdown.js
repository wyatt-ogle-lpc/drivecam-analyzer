//app/javascript/countdown.js
document.addEventListener('turbo:load', () => {
  
    const countdownEl = document.getElementById('token-countdown');
  
    if (!countdownEl) return;
  
    const expiration = parseInt(countdownEl.dataset.expiration, 10) * 1000;
  
    if (isNaN(expiration)) return;
  
    function updateCountdown() {
      const diff = expiration - Date.now();
      if (diff <= 0) {
        countdownEl.textContent = "expired";
        countdownEl.style.color = "red";
        return;
      }
      const totalSeconds = Math.floor(diff / 1000);
      const minutes = Math.floor(totalSeconds / 60);
      const seconds = totalSeconds % 60;
      countdownEl.textContent = `${minutes}m ${seconds}s`;
      setTimeout(updateCountdown, 1000);
    }
  
    updateCountdown();
  });
  
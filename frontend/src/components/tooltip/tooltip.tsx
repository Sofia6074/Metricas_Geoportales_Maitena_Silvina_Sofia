"use client";

import React, { useState, ReactNode, ReactElement } from "react";
import styles from "./tooltip.module.css";

type TooltipProps = {
  children: ReactNode;
  content: string;
  direction?: "top" | "right" | "bottom" | "left";
  delay?: number;
};

const Tooltip = ({ children, content, direction = "top", delay = 100 }: TooltipProps): ReactElement => {
  let timeout: NodeJS.Timeout;
  const [active, setActive] = useState(false);

  const showTip = () => {
    timeout = setTimeout(() => {
      setActive(true);
    }, delay);
  };

  const hideTip = () => {
    clearTimeout(timeout);
    setActive(false);
  };

  return (
    <div className={styles.tooltipWrapper} onMouseEnter={showTip} onMouseLeave={hideTip}>
      {children}
      {active && (
        <div className={`${styles.tooltipTip} ${styles[direction]}`}>
          {content}
        </div>
      )}
    </div>
  );
};

export default Tooltip;

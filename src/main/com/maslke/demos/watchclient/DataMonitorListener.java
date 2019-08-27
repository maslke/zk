package com.maslke.demos.watchclient;

public interface DataMonitorListener {
    void exists(byte[] data);

    void closing(int rc);
}

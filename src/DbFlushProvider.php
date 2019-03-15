<?php

namespace Crystoline\LaravelDbFlush;

use Crystoline\LaravelDbFlush\Console\DatabaseFlush;
use Illuminate\Support\ServiceProvider;

class DbFlushProvider extends ServiceProvider
{
    /**
     * Bootstrap the application services.
     *
     * @return void
     */
    public function boot()
    {
        $this->commands([
            DatabaseFlush::class,
        ]);
    }

    /**
     * Register the application services.
     *
     * @return void
     */
    public function register()
    {
        // register our controller

    }
}

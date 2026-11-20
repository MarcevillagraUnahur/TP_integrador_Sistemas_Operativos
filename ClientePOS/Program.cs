using System;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace ClientePOS
{
    /// <summary>
    /// Terminal de Punto de Venta (POS) Cliente - Autorizador de Tarjetas de Crédito.
    /// Trabajo Práctico Integrador - Sistemas Operativos - UNAHUR.
    /// 
    /// Conceptos clave del SO implementados:
    /// 1. SysCalls de Red: socket(), connect(), send(), recv(), close().
    /// 2. Configuración por Rubro Comercial al iniciar el proceso.
    /// 3. Desacoplamiento Cliente-Servidor: La terminal NO conoce los clientes ni sus límites;
    ///    solicita N° de Tarjeta, Código de Seguridad (CVV de 3 dígitos) e Importe, y el Servidor
    ///    central valida todo en su Sección Crítica.
    /// 4. Tolerancia a Fallos y Anulación Interactiva: Detección inmediata de caída del servidor;
    ///    si ocurre durante una venta o pago, permite ANULAR la operación con [A] o [ESC] para que
    ///    el cliente no quede esperando y sin enviar cargos al servidor.
    /// 5. Flujo de datos TCP: Delimitación de tramas mediante '\n' para evitar problemas de fragmentación.
    /// </summary>
    internal class Program
    {
        // Dirección y Puerto del servidor central autorizador
        private static string _servidorIp = "127.0.0.1";
        private const int SERVIDOR_PUERTO = 5000;

        // Identificador único de esta terminal y su Rubro Comercial asignado al abrirse
        private static string _idTerminal = "POS-01";
        private static string _rubroComercio = "";

        // Sincronización para el acceso al socket entre el hilo principal y el hilo vigilante
        private static readonly object _bloqueoSocket = new object();

        // Socket TCP del cliente
        private static Socket? _socketCliente = null;
        private static bool _clienteActivo = true;
        private static bool _ultimoEstadoConectado = false;

        static void Main(string[] args)
        {
            if (args.Length > 0 && !string.IsNullOrWhiteSpace(args[0]))
                _idTerminal = args[0].Trim();

            if (args.Length > 1 && !string.IsNullOrWhiteSpace(args[1]))
                _servidorIp = args[1].Trim();

            if (args.Length > 2 && !string.IsNullOrWhiteSpace(args[2]))
                _rubroComercio = args[2].Trim();

            ImprimirBanner();

            // =========================================================================
            // PASO 1: Al abrir la terminal POS, el operador indica de qué RUBRO es el comercio
            // =========================================================================
            if (string.IsNullOrWhiteSpace(_rubroComercio))
            {
                _rubroComercio = SolicitarRubroAlAbrir();
            }

            Console.Title = $"Terminal {_idTerminal} - Rubro: {_rubroComercio}";

            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"\n[CONFIGURACIÓN DE LA TERMINAL POS]");
            Console.WriteLine($"  • Terminal ID       : {_idTerminal}");
            Console.WriteLine($"  • Rubro del Comercio: {_rubroComercio}");
            Console.WriteLine($"  • Servidor Destino  : {_servidorIp}:{SERVIDOR_PUERTO}");
            Console.WriteLine($"  • PID en el SO      : {Environment.ProcessId}\n");
            Console.ResetColor();

            // Manejo de apagado ordenado mediante señal SIGINT (Ctrl + C)
            Console.CancelKeyPress += (sender, e) =>
            {
                Console.WriteLine("\n[SO - Señal SIGINT] Cerrando terminal POS de forma ordenada (enviando TCP FIN)...");
                _clienteActivo = false;
                e.Cancel = true;
                CerrarSocket();
            };

            // Nos conectamos inicialmente para registrar la terminal y su rubro en el servidor
            AsegurarConexion(mostrarMensajeIntento: true);

            // Hilo vigilante de fondo: detecta en tiempo real si el servidor se cae o vuelve a encenderse
            Thread hiloVigilante = new Thread(VigilarEnlaceYReconectar)
            {
                Name = "Hilo-Vigilante-Red",
                IsBackground = true
            };
            hiloVigilante.Start();

            // Iniciamos el modo Posnet interactivo
            EjecutarModoPosnetReal();
        }

        /// <summary>
        /// Hilo secundario que monitorea cada 2.5 segundos si el enlace con el servidor cayó
        /// o si el servidor volvió a levantarse para reconectar automáticamente.
        /// </summary>
        private static void VigilarEnlaceYReconectar()
        {
            while (_clienteActivo)
            {
                Thread.Sleep(2500);
                if (!_clienteActivo) break;

                lock (_bloqueoSocket)
                {
                    if (_socketCliente != null && !SocketSigueConectado(_socketCliente))
                    {
                        Console.ForegroundColor = ConsoleColor.Magenta;
                        Console.WriteLine($"\n[{DateTime.Now:HH:mm:ss.fff}] [ALERTA DE RED] Se perdió la conexión con el Servidor Central.");
                        Console.ResetColor();
                        CerrarSocket();
                        _ultimoEstadoConectado = false;
                    }

                    if (_socketCliente == null)
                    {
                        AsegurarConexion(mostrarMensajeIntento: false);
                    }
                }
            }
        }

        /// <summary>
        /// Verifica mediante Poll a nivel del Kernel si el socket recibió un cierre (FIN/RST) del servidor.
        /// </summary>
        private static bool SocketSigueConectado(Socket s)
        {
            try
            {
                if (!s.Connected) return false;
                if (s.Poll(0, SelectMode.SelectRead) && s.Available == 0)
                    return false;
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Solicita al usuario indicar a qué rubro comercial pertenece esta terminal al iniciar.
        /// </summary>
        private static string SolicitarRubroAlAbrir()
        {
            Console.WriteLine("============================================================================");
            Console.WriteLine("  CONFIGURACIÓN INICIAL: INDIQUE EL RUBRO COMERCIAL DE ESTA TERMINAL POS    ");
            Console.WriteLine("============================================================================");
            Console.WriteLine("  1. Supermercado        (Alimentos, bebidas y artículos del hogar)");
            Console.WriteLine("  2. Combustible         (Estación de servicio y carga de combustible)");
            Console.WriteLine("  3. Electrodomésticos   (Tecnología, TV, climatización y línea blanca)");
            Console.WriteLine("  4. Gastronomía         (Restaurantes, cafeterías y comida rápida)");
            Console.WriteLine("  5. Farmacia            (Medicamentos, salud y perfumería)");
            Console.WriteLine("  6. Pagos               (Caja de cobro de resumen de Tarjeta de Crédito)");
            Console.Write("Seleccione el rubro de la terminal (1 al 6, default 1): ");

            string? op = Console.ReadLine()?.Trim();
            return op switch
            {
                "2" => "Combustible",
                "3" => "Electrodomésticos",
                "4" => "Gastronomía",
                "5" => "Farmacia",
                "6" => "Pagos",
                _ => "Supermercado"
            };
        }

        /// <summary>
        /// Mantiene la conexión con el servidor. Si no está conectado, intenta reconectar.
        /// </summary>
        private static bool AsegurarConexion(bool mostrarMensajeIntento = false)
        {
            lock (_bloqueoSocket)
            {
                if (_socketCliente != null && SocketSigueConectado(_socketCliente))
                {
                    return true;
                }

                CerrarSocket();

                if (mostrarMensajeIntento)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [ESTADO] Conectando con servidor autorizador ({_servidorIp}:{SERVIDOR_PUERTO})...");
                    Console.ResetColor();
                }

                try
                {
                    // =========================================================================
                    // 1. SysCall socket(): Solicita al kernel un nuevo descriptor TCP
                    // =========================================================================
                    _socketCliente = new Socket(
                        AddressFamily.InterNetwork,
                        SocketType.Stream,
                        ProtocolType.Tcp);

                    // =========================================================================
                    // 2. SysCall connect(): Inicia el 3-Way Handshake TCP (SYN, SYN-ACK, ACK)
                    // =========================================================================
                    _socketCliente.Connect(IPAddress.Parse(_servidorIp), SERVIDOR_PUERTO);

                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine($"\n[{DateTime.Now:HH:mm:ss.fff}] [CONEXIÓN ESTABLECIDA] Terminal {_idTerminal} ({_rubroComercio}) conectada al Servidor.");
                    Console.WriteLine($"           Descriptor local (FD): {_socketCliente.Handle} | Puerto efímero: {((IPEndPoint)_socketCliente.LocalEndPoint!).Port}");
                    Console.ResetColor();

                    _ultimoEstadoConectado = true;

                    // Enviamos trama de presentación para que el Servidor y la GUI Web registren nuestro Rubro
                    string saludoRubro = $"HELLO|{_idTerminal}|{_rubroComercio}\n";
                    _socketCliente.Send(Encoding.UTF8.GetBytes(saludoRubro));

                    return true;
                }
                catch (SocketException ex)
                {
                    if (mostrarMensajeIntento || _ultimoEstadoConectado)
                    {
                        Console.ForegroundColor = ConsoleColor.DarkYellow;
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [SIN CONEXIÓN] Servidor no disponible ({ex.SocketErrorCode}).");
                        Console.ResetColor();
                    }
                    _ultimoEstadoConectado = false;
                    CerrarSocket();
                    return false;
                }
                catch (Exception ex)
                {
                    if (mostrarMensajeIntento)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"[ERROR EN CONEXIÓN] {ex.Message}");
                        Console.ResetColor();
                    }
                    _ultimoEstadoConectado = false;
                    CerrarSocket();
                    return false;
                }
            }
        }

        /// <summary>
        /// Cuando el servidor está desconectado mientras se procesa una venta o pago,
        /// muestra en pantalla la opción de ANULAR presionando [A] o [ESC] para que el cliente
        /// no quede clavado esperando. Si se anula, devuelve false y NO se envía nada al servidor.
        /// </summary>
        private static bool EsperarServidorOAnular(string etapaActual)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"\n   ⚠ [SERVIDOR DESCONECTADO DURANTE LA OPERACIÓN: {etapaActual}]");
            Console.WriteLine("     Esperando respuesta del Servidor Central...");
            Console.WriteLine("     👉 Presione [A] o [ESC] para ANULAR la operación y no dejar esperando al cliente.");
            Console.ResetColor();

            // Limpiamos teclas previas que hayan quedado en el buffer de entrada
            while (Console.KeyAvailable) Console.ReadKey(intercept: true);

            while (_clienteActivo)
            {
                // Escuchamos el teclado cada 100ms durante 1.5 segundos antes de cada reintento
                for (int i = 0; i < 15; i++)
                {
                    if (Console.KeyAvailable)
                    {
                        ConsoleKeyInfo tecla = Console.ReadKey(intercept: true);
                        if (tecla.Key == ConsoleKey.A || tecla.Key == ConsoleKey.Escape)
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine("\n   ╔══════════════════════════════════════════════════════════════════════╗");
                            Console.WriteLine("   ║ 🚫 OPERACIÓN ANULADA POR EL OPERADOR                                 ║");
                            Console.WriteLine("   ║ La transacción fue cancelada por falta de conexión con el servidor.  ║");
                            Console.WriteLine("   ║ NO se envió ningún dato ni se realizó ningún cargo en la tarjeta.    ║");
                            Console.WriteLine("   ╚══════════════════════════════════════════════════════════════════════╝");
                            Console.ResetColor();
                            return false;
                        }
                    }
                    Thread.Sleep(100);
                }

                // Verificamos si el servidor ya volvió a estar en línea
                if (AsegurarConexion(mostrarMensajeIntento: false))
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("   ✔ [SERVIDOR RECONECTADO] Continuando con la operación...");
                    Console.ResetColor();
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Envía la transacción final al servidor central.
        /// Si el servidor se desconectó, permite esperar o ANULAR la venta/pago sin enviarla.
        /// </summary>
        private static bool EnviarTransaccion(string nroTarjeta, string cvv, string tipoOperacion, decimal importe, string detalle)
        {
            string timestampIso = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.fff");
            string importeFormato = importe.ToString("F2", CultureInfo.InvariantCulture);
            string trama = $"{_idTerminal}|{_rubroComercio}|{nroTarjeta}|{cvv}|{tipoOperacion}|{importeFormato}|{timestampIso}|{detalle}\n";

            while (_clienteActivo)
            {
                lock (_bloqueoSocket)
                {
                    if (AsegurarConexion(mostrarMensajeIntento: false))
                    {
                        try
                        {
                            // =========================================================================
                            // 3. SysCall send(): Copia los bytes de la transacción al buffer del kernel
                            // =========================================================================
                            byte[] bytesTrama = Encoding.UTF8.GetBytes(trama);
                            _socketCliente!.Send(bytesTrama);

                            Console.ForegroundColor = ConsoleColor.Cyan;
                            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [ENVIANDO {tipoOperacion}] Tarjeta: {nroTarjeta} (CVV: {cvv}) | Rubro: {_rubroComercio} | ${importe:F2}");
                            Console.ResetColor();

                            // =========================================================================
                            // 4. SysCall recv(): Espera la resolución del Servidor (APROBADA o RECHAZADA)
                            // =========================================================================
                            _socketCliente.ReceiveTimeout = 3000;
                            byte[] bufferRespuesta = new byte[512];
                            int bytesRecibidos = _socketCliente.Receive(bufferRespuesta);

                            if (bytesRecibidos > 0)
                            {
                                string respuesta = Encoding.UTF8.GetString(bufferRespuesta, 0, bytesRecibidos).Trim();
                                MostrarRespuestaServidor(respuesta);
                                return true;
                            }
                            else
                            {
                                CerrarSocket();
                            }
                        }
                        catch (Exception)
                        {
                            CerrarSocket();
                        }
                    }
                }

                // Si llegamos aquí es porque el servidor se desconectó al intentar enviar:
                // Ofrecemos esperar o ANULAR la operación con [A] / [ESC].
                if (!EsperarServidorOAnular($"Envío de {tipoOperacion} por ${importe:F2}"))
                {
                    // El usuario eligió ANULAR: salimos sin enviar nada
                    return false;
                }
            }

            return false;
        }

        /// <summary>
        /// Muestra un ticket visual con el resultado (APROBADA o RECHAZADA), destacando el
        /// N° de Transacción Global Correlativo y el Saldo Disponible (ambas variables compartidas
        /// en el Heap del Servidor protegidas por la Sección Crítica).
        /// </summary>
        private static void MostrarRespuestaServidor(string respuestaRaw)
        {
            string linea = respuestaRaw.Split('\n')[0].Trim();
            string[] partes = linea.Split('|');
            string estado = partes.Length > 0 ? partes[0] : "ACK";
            string nroTxGlobal = partes.Length > 1 ? partes[1] : "#0000";
            string motivo = partes.Length > 2 ? partes[2] : linea;
            string disponible = partes.Length > 3 ? partes[3] : "$0.00";
            string hora = partes.Length > 4 ? partes[4] : "";
            string hiloSrv = partes.Length > 5 ? partes[5] : "";

            if (estado == "APROBADA")
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("   ╔══════════════════════════════════════════════════════════════════════════════╗");
                Console.WriteLine($"   ║ ✔ OPERACIÓN APROBADA           │ N° TRANSACCIÓN GLOBAL (Heap): {nroTxGlobal,-13} ║");
                Console.WriteLine($"   ║ {RecortarTexto(motivo, 30),-30} │ Disp: {disponible,-12} │ {hiloSrv,-8} ({hora})║");
                Console.WriteLine("   ╚══════════════════════════════════════════════════════════════════════════════╝");
                Console.ResetColor();
            }
            else if (estado == "RECHAZADA")
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("   ╔══════════════════════════════════════════════════════════════════════════════╗");
                Console.WriteLine($"   ║ ✖ OPERACIÓN RECHAZADA          │ N° TRANSACCIÓN GLOBAL (Heap): {nroTxGlobal,-13} ║");
                Console.WriteLine($"   ║ {RecortarTexto(motivo, 30),-30} │ Disp: {disponible,-12} │ {hiloSrv,-8} ({hora}) ║");
                Console.WriteLine("   ╚══════════════════════════════════════════════════════════════════════════════╝");
                Console.ResetColor();
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                Console.WriteLine($"           -> Respuesta Servidor: {linea}");
                Console.ResetColor();
            }
        }

        private static string RecortarTexto(string texto, int max)
        {
            if (string.IsNullOrEmpty(texto)) return "";
            return texto.Length <= max ? texto : texto.Substring(0, max);
        }

        /// <summary>
        /// Realiza una consulta al Servidor por el Socket TCP para validar cada paso (Tarjeta y CVV).
        /// Si el servidor se cayó, ofrece esperar reconexión o ANULAR la operación con [A]/[ESC].
        /// </summary>
        private static bool ConsultarValidacionPasoAPaso(string tramaConsulta, string nombrePaso, out bool operacionAnulada, out string tipoRespuesta, out string dato1, out string dato2)
        {
            operacionAnulada = false;
            tipoRespuesta = "";
            dato1 = "";
            dato2 = "";

            while (_clienteActivo)
            {
                lock (_bloqueoSocket)
                {
                    if (AsegurarConexion(mostrarMensajeIntento: false))
                    {
                        try
                        {
                            byte[] bytes = Encoding.UTF8.GetBytes(tramaConsulta + "\n");
                            _socketCliente!.Send(bytes);

                            _socketCliente.ReceiveTimeout = 3000;
                            byte[] buffer = new byte[512];
                            int leidos = _socketCliente.Receive(buffer);

                            if (leidos > 0)
                            {
                                string linea = Encoding.UTF8.GetString(buffer, 0, leidos).Split('\n')[0].Trim();
                                string[] partes = linea.Split('|');
                                tipoRespuesta = partes.Length > 0 ? partes[0] : "";
                                dato1 = partes.Length > 1 ? partes[1] : "";
                                dato2 = partes.Length > 2 ? partes[2] : "";
                                return true;
                            }
                            else
                            {
                                CerrarSocket();
                            }
                        }
                        catch
                        {
                            CerrarSocket();
                        }
                    }
                }

                // Si el servidor no está conectado, permitimos esperar o ANULAR con [A] / [ESC]
                if (!EsperarServidorOAnular(nombrePaso))
                {
                    operacionAnulada = true;
                    return false;
                }
            }

            operacionAnulada = true;
            return false;
        }

        /// <summary>
        /// Modo Posnet Real con validación en cada paso contra el Servidor y opción de Anulación ante corte de red.
        /// </summary>
        private static void EjecutarModoPosnetReal()
        {
            bool esTerminalDePagos = _rubroComercio.Equals("Pagos", StringComparison.OrdinalIgnoreCase);
            string tipoOp = esTerminalDePagos ? "PAGO" : "COMPRA";

            Console.WriteLine("============================================================================");
            if (esTerminalDePagos)
                Console.WriteLine("  CAJA DE PAGOS ACTIVA - SOLO RECEPCIÓN DE PAGOS DE RESUMEN DE TARJETA");
            else
                Console.WriteLine($"  TERMINAL POS LISTA PARA COMPRAS - RUBRO: {_rubroComercio.ToUpperInvariant()}");
            Console.WriteLine("============================================================================");

            while (_clienteActivo)
            {
                Console.WriteLine("\n----------------------------------------------------------------------------");
                bool anulada = false;

                // =====================================================================
                // PASO 1: Solicitar y validar el N° de Tarjeta contra el Servidor
                // =====================================================================
                string nroTarjetaValidado = "";
                while (_clienteActivo)
                {
                    Console.Write("1. Ingrese N° de Tarjeta (ej. XXXX-XXXX) o 'exit' para salir: ");
                    string? entradaTarjeta = Console.ReadLine()?.Trim();

                    if (string.IsNullOrWhiteSpace(entradaTarjeta))
                    {
                        ImprimirErrorLocal("Debe ingresar un número de tarjeta.");
                        continue;
                    }
                    if (entradaTarjeta.Equals("exit", StringComparison.OrdinalIgnoreCase))
                    {
                        _clienteActivo = false;
                        return;
                    }

                    if (!ConsultarValidacionPasoAPaso($"CHECK_CARD|{_idTerminal}|{entradaTarjeta}", "Validación de Tarjeta", out bool fueAnulada, out string respTipo, out string respDato1, out string respDato2))
                    {
                        if (fueAnulada)
                        {
                            anulada = true;
                            break;
                        }
                        ImprimirErrorLocal(respDato1);
                        continue;
                    }

                    if (respTipo == "OK_CARD")
                    {
                        nroTarjetaValidado = respDato1;
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"   ✔ Tarjeta verificada en el Servidor ({respDato2})");
                        Console.ResetColor();
                        break;
                    }
                    else
                    {
                        ImprimirErrorLocal(respDato1);
                    }
                }

                if (anulada || !_clienteActivo) continue;

                // =====================================================================
                // PASO 2: Solicitar y validar el Código de Seguridad (CVV) en el Servidor
                // =====================================================================
                string cvvValidado = "";
                while (_clienteActivo)
                {
                    Console.Write("2. Ingrese Código de Seguridad CVV (3 dígitos): ");
                    string? entradaCvv = Console.ReadLine()?.Trim();

                    if (string.IsNullOrWhiteSpace(entradaCvv) || entradaCvv.Length != 3 || !EsSoloDigitos(entradaCvv))
                    {
                        ImprimirErrorLocal("El Código de Seguridad (CVV) debe tener exactamente 3 dígitos numéricos.");
                        continue;
                    }

                    if (!ConsultarValidacionPasoAPaso($"CHECK_CVV|{_idTerminal}|{nroTarjetaValidado}|{entradaCvv}", "Validación de CVV", out bool fueAnulada, out string respTipo, out string respMotivo, out _))
                    {
                        if (fueAnulada)
                        {
                            anulada = true;
                            break;
                        }
                        ImprimirErrorLocal(respMotivo);
                        continue;
                    }

                    if (respTipo == "OK_CVV")
                    {
                        cvvValidado = entradaCvv;
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"   ✔ {respMotivo}");
                        Console.ResetColor();
                        break;
                    }
                    else
                    {
                        ImprimirErrorLocal(respMotivo);
                    }
                }

                if (anulada || !_clienteActivo) continue;

                // =====================================================================
                // PASO 3: Solicitar el Importe y enviar la transacción final al Servidor
                // =====================================================================
                while (_clienteActivo)
                {
                    Console.Write($"3. Ingrese el Importe de {(esTerminalDePagos ? "el PAGO" : "la COMPRA")} ($): ");
                    string? lineaImporte = Console.ReadLine()?.Trim();

                    if (!decimal.TryParse(lineaImporte, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal importe) || importe <= 0)
                    {
                        ImprimirErrorLocal("Importe inválido. Debe ingresar un monto numérico mayor a 0 (ej: 15400.50).");
                        continue;
                    }

                    string detalle = esTerminalDePagos ? "Pago de Resumen" : $"Compra {_rubroComercio}";
                    EnviarTransaccion(nroTarjetaValidado, cvvValidado, tipoOp, importe, detalle);
                    break;
                }
            }
        }

        private static bool EsSoloDigitos(string texto)
        {
            foreach (char c in texto)
            {
                if (!char.IsDigit(c)) return false;
            }
            return true;
        }

        private static void ImprimirErrorLocal(string mensaje)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"   [ERROR EN TERMINAL POS] {mensaje}");
            Console.ResetColor();
        }

        private static void CerrarSocket()
        {
            if (_socketCliente != null)
            {
                try { _socketCliente.Shutdown(SocketShutdown.Both); } catch { }
                try { _socketCliente.Close(); } catch { }
                _socketCliente = null;
            }
        }

        private static void ImprimirBanner()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("============================================================================");
            Console.WriteLine("     SISTEMA AUTORIZADOR DE TARJETAS POS - TERMINAL DE COBRO (CLIENTE)      ");
            Console.WriteLine("           UNAHUR - Sistemas Operativos 2C 2026 - Opción 5 (Sockets)        ");
            Console.WriteLine("============================================================================");
            Console.ResetColor();
        }
    }
}

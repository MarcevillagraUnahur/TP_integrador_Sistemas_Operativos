using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;

namespace ServidorPOS
{
    /// <summary>
    /// Representa la cuenta de una Tarjeta de Crédito alojada en la memoria compartida (Heap) del Servidor.
    /// </summary>
    public class CuentaTarjeta
    {
        public string Numero { get; set; } = "";
        public string CodigoSeguridad { get; set; } = "";
        public string Titular { get; set; } = "";
        public decimal LimiteTotal { get; set; }
        public decimal DeudaActual { get; set; }
        public decimal SaldoDisponible => Math.Max(0, LimiteTotal - DeudaActual);
        public int ComprasAprobadas { get; set; }
        public int ComprasRechazadas { get; set; }
    }

    /// <summary>
    /// Acumula las métricas de ventas de un rubro comercial específico.
    /// </summary>
    public class EstadisticaRubro
    {
        public string Rubro { get; set; } = "";
        public int CantidadCompras { get; set; }
        public decimal MontoAcumulado { get; set; }
    }

    /// <summary>
    /// Registra la información de una terminal POS actualmente conectada al servidor.
    /// </summary>
    public class InfoTerminalActiva
    {
        public int IdInterno { get; set; }
        public string CodigoTerminal { get; set; } = "";
        public string Rubro { get; set; } = "Sin especificar";
        public string DireccionIp { get; set; } = "";
        public long DescriptorSocket { get; set; }
        public int HiloSoId { get; set; }
        public int TransaccionesProcesadas { get; set; }
        public string HoraConexion { get; set; } = "";

        [JsonIgnore]
        public Socket? SocketAsociado { get; set; }
    }

    /// <summary>
    /// Historial de una transacción procesada para mostrar en el Dashboard Web.
    /// </summary>
    public class RegistroTransaccion
    {
        public int Numero { get; set; }
        public string Hora { get; set; } = "";
        public string Terminal { get; set; } = "";
        public string Rubro { get; set; } = "";
        public string Tarjeta { get; set; } = "";
        public string Titular { get; set; } = "";
        public string Tipo { get; set; } = ""; // COMPRA o PAGO
        public decimal Importe { get; set; }
        public string Estado { get; set; } = ""; // APROBADA o RECHAZADA
        public string Motivo { get; set; } = "";
        public decimal DisponibleRestante { get; set; }
        public int HiloId { get; set; }
    }

    /// <summary>
    /// Estructura para persistir en disco (almacenamiento no volátil del SO) el estado del Heap
    /// ante caídas o reinicios del proceso Servidor.
    /// </summary>
    public class EstadoPersistenteServidor
    {
        public int TotalTransacciones { get; set; }
        public int TotalAprobadas { get; set; }
        public int TotalRechazadas { get; set; }
        public decimal MontoTotalComprasAprobadas { get; set; }
        public decimal MontoTotalPagosRecibidos { get; set; }
        public List<CuentaTarjeta> Tarjetas { get; set; } = new List<CuentaTarjeta>();
        public List<EstadisticaRubro> Rubros { get; set; } = new List<EstadisticaRubro>();
        public List<RegistroTransaccion> Historial { get; set; } = new List<RegistroTransaccion>();
    }

    /// <summary>
    /// Servidor Autorizador de Tarjetas de Crédito y Telemetría para Red de Puntos de Venta (POS).
    /// Trabajo Práctico Integrador - Sistemas Operativos - UNAHUR.
    /// 
    /// Conceptos clave del SO implementados:
    /// 1. SysCalls de Red: socket(), bind(), listen(), accept(), recv(), send(), close().
    /// 2. Descriptores de Socket (File Descriptors en Linux / SOCKET Handles en Windows).
    /// 3. Concurrencia mediante Hilos (Threads): Cada terminal POS es atendida en un hilo separado.
    /// 4. Sección Crítica y Sincronización (lock/Monitor): Protege los saldos de las tarjetas,
    ///    evitando Condiciones de Carrera (Race Conditions) si dos terminales cobran a la misma tarjeta a la vez.
    /// 5. Flujo de Bytes vs. Mensajes (TCP Stream): Delimitador '\n' para evitar fragmentación.
    /// 6. Tolerancia a fallos: Detección de desconexión limpia (recv = 0) y abrupta (ECONNRESET / 10054).
    /// 7. Interfaz Gráfica Web (GUI): Segundo socket en el puerto 8080 que sirve el panel de control en vivo.
    /// </summary>
    internal class Program
    {
        // Puerto TCP principal para las Terminales POS
        private const int PUERTO_POS = 5000;

        // Puerto TCP secundario para el Dashboard Web (GUI en el navegador)
        private const int PUERTO_WEB_GUI = 8080;

        // =========================================================================
        // ESTRUCTURAS EN EL HEAP (MEMORIA COMPARTIDA ENTRE TODOS LOS HILOS)
        // =========================================================================
        // Todas estas variables están en el segmento Heap del proceso y son accedidas
        // por múltiples hilos simultáneamente. Por eso se protegen con _bloqueoSeccionCritica.
        private static readonly object _bloqueoSeccionCritica = new object();

        private static int _clientesConectados = 0;
        private static int _totalTransacciones = 0;
        private static int _totalAprobadas = 0;
        private static int _totalRechazadas = 0;
        private static decimal _montoTotalComprasAprobadas = 0m;
        private static decimal _montoTotalPagosRecibidos = 0m;

        // Base de datos en memoria (Heap) de Tarjetas de Crédito
        private static readonly Dictionary<string, CuentaTarjeta> _tarjetas = new Dictionary<string, CuentaTarjeta>();

        // Estadísticas por rubro comercial
        private static readonly Dictionary<string, EstadisticaRubro> _estadisticasRubros = new Dictionary<string, EstadisticaRubro>();

        // Terminales POS conectadas actualmente
        private static readonly Dictionary<int, InfoTerminalActiva> _terminalesActivas = new Dictionary<int, InfoTerminalActiva>();

        // Últimas 40 transacciones para visualizar en el monitor en vivo
        private static readonly List<RegistroTransaccion> _historialTransacciones = new List<RegistroTransaccion>();

        // Archivo en disco (Memoria No Volátil) para recuperar el estado si el servidor se cae y vuelve a levantar
        private static readonly string _rutaArchivoPersistencia = Path.Combine(AppContext.BaseDirectory, "estado_persistente.json");

        // Control del ciclo de vida del servidor
        private static bool _servidorActivo = true;

        static void Main(string[] args)
        {
            Console.Title = "Servidor Autorizador de Tarjetas POS (Central)";
            InicializarDatosEnMemoria();
            ImprimirBanner();

            // Iniciamos en un hilo de fondo el servidor Web GUI (puerto 8080)
            Thread hiloWebGui = new Thread(IniciarServidorWebGui)
            {
                Name = "Hilo-Servidor-Web-GUI",
                IsBackground = true
            };
            hiloWebGui.Start();

            // =========================================================================
            // 1. SysCall socket(): Creación del descriptor de socket en el Kernel
            // =========================================================================
            Socket socketEscucha = new Socket(
                AddressFamily.InterNetwork,
                SocketType.Stream,
                ProtocolType.Tcp);

            // Opción SO_REUSEADDR: Permite reutilizar el puerto inmediatamente si el servidor se reinicia
            socketEscucha.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

            try
            {
                // =========================================================================
                // 2. SysCall bind(): Asociación del socket a la IP 0.0.0.0 y Puerto 5000
                // =========================================================================
                IPEndPoint puntoLocal = new IPEndPoint(IPAddress.Any, PUERTO_POS);
                socketEscucha.Bind(puntoLocal);
                Console.WriteLine($"[KERNEL - bind()] Socket POS asociado al puerto {PUERTO_POS} (IP: 0.0.0.0).");

                // =========================================================================
                // 3. SysCall listen(): Poner el socket en modo escucha pasiva
                // =========================================================================
                socketEscucha.Listen(100);
                Console.WriteLine($"[KERNEL - listen()] Esperando conexiones de terminales POS...");
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"[GUI WEB ACTIVA]   Abra en su navegador: http://localhost:{PUERTO_WEB_GUI}");
                Console.ResetColor();
                Console.WriteLine(new string('-', 88));

                // Manejo de apagado ordenado mediante señal SIGINT (Ctrl + C)
                Console.CancelKeyPress += (sender, e) =>
                {
                    Console.WriteLine("\n[SO - Señal SIGINT] Deteniendo servidor de forma ordenada...");
                    _servidorActivo = false;
                    e.Cancel = true;
                    try { socketEscucha.Close(); } catch { }
                };

                // Bucle principal de aceptación de conexiones de terminales POS
                int idClienteSecuencial = 1;
                while (_servidorActivo)
                {
                    try
                    {
                        // =========================================================================
                        // 4. SysCall accept(): Espera bloqueante de nuevas terminales POS
                        // =========================================================================
                        Socket socketCliente = socketEscucha.Accept();

                        int idAsignado = idClienteSecuencial++;
                        string ipRemota = socketCliente.RemoteEndPoint?.ToString() ?? "Desconocido";
                        long handleSocket = socketCliente.Handle.ToInt64();

                        lock (_bloqueoSeccionCritica)
                        {
                            _clientesConectados++;
                            _terminalesActivas[idAsignado] = new InfoTerminalActiva
                            {
                                IdInterno = idAsignado,
                                CodigoTerminal = $"POS-{idAsignado:D2}",
                                Rubro = "Conectando...",
                                DireccionIp = ipRemota,
                                DescriptorSocket = handleSocket,
                                HiloSoId = 0,
                                TransaccionesProcesadas = 0,
                                HoraConexion = DateTime.Now.ToString("HH:mm:ss"),
                                SocketAsociado = socketCliente
                            };
                        }

                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [NUEVA CONEXIÓN] Terminal #{idAsignado} desde {ipRemota}");
                        Console.WriteLine($"           Descriptor Socket (FD): {handleSocket} | Terminales activas: {_clientesConectados}");
                        Console.ResetColor();

                        // =========================================================================
                        // Concurrencia: Atender a cada terminal POS en un Hilo independiente (Thread)
                        // =========================================================================
                        Thread hiloCliente = new Thread(() => AtenderCliente(socketCliente, idAsignado, ipRemota))
                        {
                            Name = $"Hilo-Terminal-{idAsignado}",
                            IsBackground = true
                        };
                        hiloCliente.Start();
                    }
                    catch (SocketException ex)
                    {
                        if (!_servidorActivo) break;
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"[ERROR accept()] {ex.Message} (Código: {ex.SocketErrorCode})");
                        Console.ResetColor();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"[FATAL] Error en el servidor: {ex.Message}");
                Console.ResetColor();
            }
            finally
            {
                socketEscucha.Close();
                Console.WriteLine("[SERVIDOR] Socket de escucha cerrado. Servidor finalizado.");
            }
        }

        /// <summary>
        /// Carga en memoria (Heap) las tarjetas de crédito y estadísticas.
        /// Si existe un respaldo previo en disco tras una caída del servidor (y no se pidió reset),
        /// recupera automáticamente los saldos, el contador de transacciones y el historial del Dashboard.
        /// </summary>
        private static void InicializarDatosEnMemoria(bool forzarReset = false)
        {
            lock (_bloqueoSeccionCritica)
            {
                if (!forzarReset && File.Exists(_rutaArchivoPersistencia))
                {
                    try
                    {
                        string json = File.ReadAllText(_rutaArchivoPersistencia, Encoding.UTF8);
                        var recuperado = JsonSerializer.Deserialize<EstadoPersistenteServidor>(json);
                        if (recuperado != null && recuperado.Tarjetas.Count > 0)
                        {
                            _totalTransacciones = recuperado.TotalTransacciones;
                            _totalAprobadas = recuperado.TotalAprobadas;
                            _totalRechazadas = recuperado.TotalRechazadas;
                            _montoTotalComprasAprobadas = recuperado.MontoTotalComprasAprobadas;
                            _montoTotalPagosRecibidos = recuperado.MontoTotalPagosRecibidos;

                            _tarjetas.Clear();
                            foreach (var t in recuperado.Tarjetas)
                                _tarjetas[t.Numero] = t;

                            _estadisticasRubros.Clear();
                            foreach (var r in recuperado.Rubros)
                                _estadisticasRubros[r.Rubro] = r;

                            _historialTransacciones.Clear();
                            _historialTransacciones.AddRange(recuperado.Historial);

                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine($"[SO - PERSISTENCIA EN DISCO] Se detectó información previa al reinicio.");
                            Console.WriteLine($"      Recuperadas {_totalTransacciones} transacciones, saldos y estadísticas en el Heap.");
                            Console.ResetColor();
                            return;
                        }
                    }
                    catch { }
                }

                // Si se pidió Reset o es el primer inicio limpio:
                try
                {
                    if (File.Exists(_rutaArchivoPersistencia))
                        File.Delete(_rutaArchivoPersistencia);
                }
                catch { }

                _totalTransacciones = 0;
                _totalAprobadas = 0;
                _totalRechazadas = 0;
                _montoTotalComprasAprobadas = 0m;
                _montoTotalPagosRecibidos = 0m;
                _tarjetas.Clear();
                _estadisticasRubros.Clear();
                _historialTransacciones.Clear();

                _tarjetas["4509-1001"] = new CuentaTarjeta { Numero = "4509-1001", CodigoSeguridad = "111", Titular = "Ana Gómez", LimiteTotal = 60000m, DeudaActual = 10000m };
                _tarjetas["4509-1002"] = new CuentaTarjeta { Numero = "4509-1002", CodigoSeguridad = "222", Titular = "Carlos Pérez", LimiteTotal = 90000m, DeudaActual = 15000m };
                _tarjetas["4509-1003"] = new CuentaTarjeta { Numero = "4509-1003", CodigoSeguridad = "333", Titular = "Lucía Fernández", LimiteTotal = 40000m, DeudaActual = 25000m };
                _tarjetas["4509-1004"] = new CuentaTarjeta { Numero = "4509-1004", CodigoSeguridad = "444", Titular = "Martín Rodríguez", LimiteTotal = 120000m, DeudaActual = 20000m };
                _tarjetas["4509-1005"] = new CuentaTarjeta { Numero = "4509-1005", CodigoSeguridad = "555", Titular = "Sofía López", LimiteTotal = 50000m, DeudaActual = 5000m };

                string[] rubrosIniciales = { "Supermercado", "Combustible", "Electrodomésticos", "Gastronomía", "Farmacia" };
                foreach (string r in rubrosIniciales)
                {
                    _estadisticasRubros[r] = new EstadisticaRubro { Rubro = r, CantidadCompras = 0, MontoAcumulado = 0m };
                }
            }
        }

        /// <summary>
        /// Persiste en almacenamiento secundario (disco) el estado actual del Heap.
        /// Se invoca dentro del lock para garantizar consistencia ante caídas abruptas del servidor.
        /// </summary>
        private static void GuardarEstadoEnDisco()
        {
            try
            {
                var snapshot = new EstadoPersistenteServidor
                {
                    TotalTransacciones = _totalTransacciones,
                    TotalAprobadas = _totalAprobadas,
                    TotalRechazadas = _totalRechazadas,
                    MontoTotalComprasAprobadas = _montoTotalComprasAprobadas,
                    MontoTotalPagosRecibidos = _montoTotalPagosRecibidos,
                    Tarjetas = new List<CuentaTarjeta>(_tarjetas.Values),
                    Rubros = new List<EstadisticaRubro>(_estadisticasRubros.Values),
                    Historial = new List<RegistroTransaccion>(_historialTransacciones)
                };

                string json = JsonSerializer.Serialize(snapshot);
                File.WriteAllText(_rutaArchivoPersistencia, json, Encoding.UTF8);
            }
            catch { }
        }

        /// <summary>
        /// Rutina ejecutada por cada Hilo trabajador para comunicarse con una terminal POS específica.
        /// </summary>
        private static void AtenderCliente(Socket socketCliente, int idTerminal, string direccionCliente)
        {
            // Buffer local alojado en el Stack exclusivo de este hilo
            byte[] bufferLectura = new byte[1024];
            StringBuilder acumuladorMensajes = new StringBuilder();
            int threadId = Environment.CurrentManagedThreadId;

            lock (_bloqueoSeccionCritica)
            {
                if (_terminalesActivas.TryGetValue(idTerminal, out var info))
                {
                    info.HiloSoId = threadId;
                }
            }

            Console.WriteLine($"           [HILO SO {threadId}] Hilo dedicado asignado para atender Terminal #{idTerminal}.");

            try
            {
                while (_servidorActivo)
                {
                    // =========================================================================
                    // 5. SysCall recv(): Lectura bloqueante de bytes desde el socket del cliente
                    // =========================================================================
                    int bytesLeidos = socketCliente.Receive(bufferLectura, 0, bufferLectura.Length, SocketFlags.None);

                    if (bytesLeidos == 0)
                    {
                        // recv() == 0 indica que el cliente envió un paquete FIN (cierre limpio)
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [DESCONEXIÓN ORDENADA] Terminal #{idTerminal} ({direccionCliente}) cerró la conexión (FIN/EOF).");
                        Console.ResetColor();
                        break;
                    }

                    string fragmentoRecibido = Encoding.UTF8.GetString(bufferLectura, 0, bytesLeidos);
                    acumuladorMensajes.Append(fragmentoRecibido);

                    // Delimitación de Mensajes en Flujo de Bytes TCP usando '\n'
                    string contenidoAcumulado = acumuladorMensajes.ToString();
                    int indiceSaltoLinea;

                    while ((indiceSaltoLinea = contenidoAcumulado.IndexOf('\n')) >= 0)
                    {
                        string tramaCompleta = contenidoAcumulado.Substring(0, indiceSaltoLinea).Trim();
                        contenidoAcumulado = contenidoAcumulado.Substring(indiceSaltoLinea + 1);
                        acumuladorMensajes.Clear();
                        acumuladorMensajes.Append(contenidoAcumulado);

                        if (!string.IsNullOrWhiteSpace(tramaCompleta))
                        {
                            ProcesarMensajeCliente(tramaCompleta, idTerminal, socketCliente);
                        }
                    }
                }
            }
            catch (SocketException ex)
            {
                // =========================================================================
                // Tolerancia a Fallos: Detección de Desconexión Abrupta (ECONNRESET / 10054)
                // =========================================================================
                Console.ForegroundColor = ConsoleColor.Magenta;
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [TOLERANCIA A FALLOS - DESCONEXIÓN ABRUPTA]");
                Console.WriteLine($"           Terminal #{idTerminal} ({direccionCliente}) perdió el enlace intempestivamente.");
                Console.WriteLine($"           Error de Socket: {ex.SocketErrorCode} (Código nativo SO: {ex.NativeErrorCode})");
                Console.WriteLine($"           -> El servidor continúa 100% operativo para las demás terminales.");
                Console.ResetColor();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"[ERROR INESPERADO en Terminal #{idTerminal}] {ex.Message}");
                Console.ResetColor();
            }
            finally
            {
                // =========================================================================
                // 6. SysCall close(): Liberación del descriptor de socket en el Kernel
                // =========================================================================
                try { socketCliente.Shutdown(SocketShutdown.Both); } catch { }
                socketCliente.Close();

                lock (_bloqueoSeccionCritica)
                {
                    _clientesConectados--;
                    _terminalesActivas.Remove(idTerminal);
                }

                Console.ForegroundColor = ConsoleColor.DarkGray;
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [RECURSOS LIBERADOS] Socket de Terminal #{idTerminal} cerrado. Clientes activos: {_clientesConectados}\n");
                Console.ResetColor();
            }
        }

        /// <summary>
        /// Normaliza el número de tarjeta para aceptar tanto "4509-1001" como "45091001".
        /// </summary>
        private static string NormalizarNumeroTarjeta(string entrada)
        {
            string limpio = entrada.Trim();
            if (limpio.Length == 8 && !limpio.Contains('-'))
            {
                return limpio.Substring(0, 4) + "-" + limpio.Substring(4, 4);
            }
            return limpio;
        }

        /// <summary>
        /// Procesa mensajes de presentación, validaciones paso a paso (Tarjeta y CVV) y transacciones finales.
        /// Formatos soportados:
        /// 1. Presentación inicial: "HELLO|ID_TERMINAL|RUBRO"
        /// 2. Chequeo Paso 1 (Tarjeta): "CHECK_CARD|ID_TERMINAL|NRO_TARJETA"
        /// 3. Chequeo Paso 2 (CVV):     "CHECK_CVV|ID_TERMINAL|NRO_TARJETA|CVV"
        /// 4. Transacción Paso 3:       "ID_TERMINAL|RUBRO|NRO_TARJETA|CVV|TIPO_OP|IMPORTE|TIMESTAMP|DETALLE"
        /// </summary>
        private static void ProcesarMensajeCliente(string trama, int idTerminal, Socket socketCliente)
        {
            string[] campos = trama.Split('|');

            // 1. Mensaje de presentación inicial cuando se abre la terminal POS
            if (campos.Length >= 3 && campos[0] == "HELLO")
            {
                string codigoPos = campos[1];
                string rubroPos = campos[2];
                lock (_bloqueoSeccionCritica)
                {
                    if (_terminalesActivas.TryGetValue(idTerminal, out var info))
                    {
                        info.CodigoTerminal = codigoPos;
                        info.Rubro = rubroPos;
                    }
                }
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [REGISTRO RUBRO] Terminal #{idTerminal} identificada como '{codigoPos}' -> Rubro: {rubroPos}");
                Console.ResetColor();
                return;
            }

            // 2. Chequeo en Paso 1: Validar si el N° de tarjeta existe antes de pedir el CVV
            if (campos.Length >= 3 && campos[0] == "CHECK_CARD")
            {
                string nroIngresado = NormalizarNumeroTarjeta(campos[2]);
                string respuestaCard;

                lock (_bloqueoSeccionCritica)
                {
                    if (_tarjetas.TryGetValue(nroIngresado, out CuentaTarjeta? cuenta))
                    {
                        respuestaCard = $"OK_CARD|{cuenta.Numero}|Titular: {cuenta.Titular}\n";
                    }
                    else
                    {
                        respuestaCard = $"ERR_CARD|ERROR: TARJETA INEXISTENTE ({campos[2].Trim()})\n";
                    }
                }

                socketCliente.Send(Encoding.UTF8.GetBytes(respuestaCard));
                return;
            }

            // 3. Chequeo en Paso 2: Validar si el CVV de 3 dígitos es correcto antes de pedir el importe
            if (campos.Length >= 4 && campos[0] == "CHECK_CVV")
            {
                string nroIngresado = NormalizarNumeroTarjeta(campos[2]);
                string cvvIngresado = campos[3].Trim();
                string respuestaCvv;

                lock (_bloqueoSeccionCritica)
                {
                    if (!_tarjetas.TryGetValue(nroIngresado, out CuentaTarjeta? cuenta))
                    {
                        respuestaCvv = $"ERR_CVV|ERROR: TARJETA INEXISTENTE\n";
                    }
                    else if (cuenta.CodigoSeguridad != cvvIngresado)
                    {
                        respuestaCvv = $"ERR_CVV|ERROR: CÓDIGO DE SEGURIDAD (CVV) INCORRECTO\n";
                    }
                    else
                    {
                        respuestaCvv = $"OK_CVV|Código CVV verificado correctamente\n";
                    }
                }

                socketCliente.Send(Encoding.UTF8.GetBytes(respuestaCvv));
                return;
            }

            // 4. Procesamiento de transacción final (Paso 3: Importe y Límite)
            DateTime horaRecepcionExacta = DateTime.Now;

            string idTerminalReportado = campos.Length > 0 ? campos[0] : $"POS-{idTerminal:D2}";
            string rubro = campos.Length > 1 ? campos[1] : "Supermercado";
            string nroTarjeta = campos.Length > 2 ? NormalizarNumeroTarjeta(campos[2]) : "";
            string cvvRecibido = campos.Length > 3 ? campos[3].Trim() : "";
            string tipoOperacion = campos.Length > 4 ? campos[4].Trim().ToUpperInvariant() : "COMPRA";
            decimal importe = 0m;
            if (campos.Length > 5)
            {
                decimal.TryParse(campos[5], NumberStyles.Any, CultureInfo.InvariantCulture, out importe);
            }
            string timestampOrigen = campos.Length > 6 ? campos[6] : horaRecepcionExacta.ToString("yyyy-MM-ddTHH:mm:ss.fff");
            string detalle = campos.Length > 7 ? campos[7] : "Operación POS";

            int numeroTx;
            string estadoResolucion;
            string motivoResolucion;
            string titularTarjeta;
            decimal saldoDisponibleFinal;
            int threadId = Environment.CurrentManagedThreadId;

            // =========================================================================
            // SECCIÓN CRÍTICA: Validación de Tarjeta, CVV, Límites y Estadísticas
            // =========================================================================
            lock (_bloqueoSeccionCritica)
            {
                _totalTransacciones++;
                numeroTx = _totalTransacciones;

                // Actualizamos los datos de la terminal activa
                if (_terminalesActivas.TryGetValue(idTerminal, out var infoTerm))
                {
                    infoTerm.CodigoTerminal = idTerminalReportado;
                    infoTerm.Rubro = rubro;
                    infoTerm.TransaccionesProcesadas++;
                }

                // VALIDACIÓN 1: Verificar si el N° de tarjeta existe en el padrón del Servidor
                if (!_tarjetas.TryGetValue(nroTarjeta, out CuentaTarjeta? cuenta))
                {
                    _totalRechazadas++;
                    titularTarjeta = "Desconocido";
                    estadoResolucion = "RECHAZADA";
                    motivoResolucion = $"ERROR: TARJETA INEXISTENTE ({nroTarjeta})";
                    saldoDisponibleFinal = 0m;
                }
                // VALIDACIÓN 2: Verificar si el código de seguridad (CVV de 3 dígitos) coincide
                else if (cuenta.CodigoSeguridad != cvvRecibido)
                {
                    cuenta.ComprasRechazadas++;
                    _totalRechazadas++;
                    titularTarjeta = cuenta.Titular;
                    estadoResolucion = "RECHAZADA";
                    motivoResolucion = $"ERROR: CÓDIGO DE SEGURIDAD (CVV) INVÁLIDO";
                    saldoDisponibleFinal = cuenta.SaldoDisponible;
                }
                // VALIDACIÓN 3: Verificar que el importe sea mayor a cero
                else if (importe <= 0)
                {
                    cuenta.ComprasRechazadas++;
                    _totalRechazadas++;
                    titularTarjeta = cuenta.Titular;
                    estadoResolucion = "RECHAZADA";
                    motivoResolucion = "ERROR: IMPORTE INVÁLIDO";
                    saldoDisponibleFinal = cuenta.SaldoDisponible;
                }
                else
                {
                    titularTarjeta = cuenta.Titular;
                    bool esTerminalPagos = rubro.Equals("Pagos", StringComparison.OrdinalIgnoreCase);

                    if (tipoOperacion == "PAGO" && !esTerminalPagos)
                    {
                        cuenta.ComprasRechazadas++;
                        _totalRechazadas++;
                        estadoResolucion = "RECHAZADA";
                        motivoResolucion = "ERROR: TERMINAL NO HABILITADA PARA PAGOS";
                        saldoDisponibleFinal = cuenta.SaldoDisponible;
                    }
                    else if (esTerminalPagos)
                    {
                        // Únicamente la terminal de Pagos puede acreditar un PAGO que libere límite disponible
                        tipoOperacion = "PAGO";
                        cuenta.DeudaActual = Math.Max(0m, cuenta.DeudaActual - importe);
                        _montoTotalPagosRecibidos += importe;
                        _totalAprobadas++;
                        estadoResolucion = "APROBADA";
                        motivoResolucion = $"Pago acreditado ({titularTarjeta})";
                        saldoDisponibleFinal = cuenta.SaldoDisponible;
                    }
                    else
                    {
                        // El resto de los rubros solo pueden realizar COMPRAS:
                        tipoOperacion = "COMPRA";
                        if (importe <= cuenta.SaldoDisponible)
                        {
                            cuenta.DeudaActual += importe;
                            cuenta.ComprasAprobadas++;
                            _totalAprobadas++;
                            _montoTotalComprasAprobadas += importe;

                            // Sumamos la compra a la estadística del rubro correspondiente
                            if (!_estadisticasRubros.TryGetValue(rubro, out EstadisticaRubro? estRubro))
                            {
                                estRubro = new EstadisticaRubro { Rubro = rubro };
                                _estadisticasRubros[rubro] = estRubro;
                            }
                            estRubro.CantidadCompras++;
                            estRubro.MontoAcumulado += importe;

                            estadoResolucion = "APROBADA";
                            motivoResolucion = $"Autorizada ({titularTarjeta})";
                            saldoDisponibleFinal = cuenta.SaldoDisponible;
                        }
                        else
                        {
                            // Límite insuficiente: se rechaza la compra
                            cuenta.ComprasRechazadas++;
                            _totalRechazadas++;
                            estadoResolucion = "RECHAZADA";
                            motivoResolucion = $"ERROR: LÍMITE INSUFICIENTE ({titularTarjeta})";
                            saldoDisponibleFinal = cuenta.SaldoDisponible;
                        }
                    }
                }

                // Guardamos en el historial para el Dashboard Web (máximo 40 registros recientes)
                _historialTransacciones.Insert(0, new RegistroTransaccion
                {
                    Numero = numeroTx,
                    Hora = horaRecepcionExacta.ToString("HH:mm:ss.fff"),
                    Terminal = idTerminalReportado,
                    Rubro = rubro,
                    Tarjeta = nroTarjeta,
                    Titular = titularTarjeta,
                    Tipo = tipoOperacion,
                    Importe = importe,
                    Estado = estadoResolucion,
                    Motivo = motivoResolucion,
                    DisponibleRestante = saldoDisponibleFinal,
                    HiloId = threadId
                });

                if (_historialTransacciones.Count > 40)
                {
                    _historialTransacciones.RemoveAt(_historialTransacciones.Count - 1);
                }

                // Respaldamos el estado del Heap en disco (Memoria No Volátil) para no perder
                // el correlativo de transacciones ni los saldos si el servidor se apaga y vuelve a encender
                GuardarEstadoEnDisco();
            }

            // Impresión sincronizada en consola (stdout es un recurso compartido entre hilos)
            lock (_bloqueoSeccionCritica)
            {
                if (estadoResolucion == "APROBADA")
                    Console.ForegroundColor = tipoOperacion == "PAGO" ? ConsoleColor.Green : ConsoleColor.Cyan;
                else
                    Console.ForegroundColor = ConsoleColor.Red;

                Console.WriteLine($"╔══════════════════════════════════════════════════════════════════════════════════════╗");
                Console.WriteLine($"║ TX #{numeroTx,-4} [{estadoResolucion,-9}] │ {tipoOperacion,-6} │ Hora Exacta: {horaRecepcionExacta:yyyy-MM-dd HH:mm:ss.fff} │ Hilo: {threadId,3} ║");
                Console.WriteLine($"╠══════════════════════════════════════════════════════════════════════════════════════╣");
                Console.WriteLine($"║ Terminal: {idTerminalReportado,-14} │ Rubro Comercio: {rubro,-15} │ Concepto: {Recortar(detalle, 16),-16} ║");
                Console.WriteLine($"║ Tarjeta:  {nroTarjeta,-8} │ Titular:        {titularTarjeta,-20} │ Importe:  ${importe,12:F2} ║");
                Console.WriteLine($"║ Motivo:   {motivoResolucion,-38} │ Disponible Tarjeta: ${saldoDisponibleFinal,12:F2} ║");
                Console.WriteLine($"╚══════════════════════════════════════════════════════════════════════════════════════╝");
                Console.ResetColor();
            }

            // =========================================================================
            // SysCall send(): Envío del resultado de autorización hacia la terminal POS
            // Incluye el N° de Transacción Global (#0001, #0002...) y el Saldo Disponible,
            // ambos leídos y actualizados en el Heap compartido dentro de la Sección Crítica.
            // =========================================================================
            try
            {
                string dispStr = saldoDisponibleFinal.ToString("F2", CultureInfo.InvariantCulture);
                string mensajeRespuesta = $"{estadoResolucion}|#{numeroTx:D4}|{motivoResolucion}|${dispStr}|{horaRecepcionExacta:HH:mm:ss.fff}|Hilo #{threadId}\n";
                byte[] bytesRespuesta = Encoding.UTF8.GetBytes(mensajeRespuesta);
                socketCliente.Send(bytesRespuesta);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AVISO] No se pudo enviar respuesta a la Terminal #{idTerminal}: {ex.Message}");
            }
        }

        // =========================================================================
        // SERVIDOR WEB GUI (PUERTO 8080) USANDO LAS MISMAS SYSCALLS DE SOCKETS
        // =========================================================================
        /// <summary>
        /// Levanta un socket en el puerto 8080 para servir el Dashboard HTML y los datos JSON en vivo.
        /// Usa exactamente las mismas llamadas al sistema (socket, bind, listen, accept, recv, send)
        /// estudiadas en la materia, por lo que no requiere librerías externas ni permisos especiales.
        /// </summary>
        private static void IniciarServidorWebGui()
        {
            Socket socketWeb = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socketWeb.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

            try
            {
                socketWeb.Bind(new IPEndPoint(IPAddress.Any, PUERTO_WEB_GUI));
                socketWeb.Listen(50);

                while (_servidorActivo)
                {
                    Socket clienteHttp = socketWeb.Accept();
                    ThreadPool.QueueUserWorkItem(_ => AtenderPeticionHttpGui(clienteHttp));
                }
            }
            catch (Exception ex)
            {
                if (_servidorActivo)
                {
                    Console.WriteLine($"[AVISO GUI WEB] No se pudo iniciar el puerto {PUERTO_WEB_GUI}: {ex.Message}");
                }
            }
            finally
            {
                try { socketWeb.Close(); } catch { }
            }
        }

        /// <summary>
        /// Atiende una petición del navegador web (entrega el HTML o el JSON de estado en tiempo real).
        /// </summary>
        private static void AtenderPeticionHttpGui(Socket clienteHttp)
        {
            try
            {
                clienteHttp.ReceiveTimeout = 2000;
                byte[] buffer = new byte[4096];
                int leidos = clienteHttp.Receive(buffer);
                if (leidos <= 0) return;

                string peticion = Encoding.UTF8.GetString(buffer, 0, leidos);
                string primeraLinea = peticion.Split('\r', '\n')[0];
                string[] partes = primeraLinea.Split(' ');
                string ruta = partes.Length > 1 ? partes[1] : "/";

                if (ruta.StartsWith("/api/estado"))
                {
                    string json = GenerarJsonEstadoActual();
                    EnviarRespuestaHttp(clienteHttp, "200 OK", "application/json; charset=utf-8", Encoding.UTF8.GetBytes(json));
                }
                else if (ruta.StartsWith("/api/reset"))
                {
                    // Permite resetear la memoria del servidor y borrar el respaldo en disco para iniciar de cero
                    InicializarDatosEnMemoria(forzarReset: true);
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [RESET DE MEMORIA] Se reinició el contador, saldos e historial a valores iniciales desde el Dashboard.");
                    Console.ResetColor();
                    EnviarRespuestaHttp(clienteHttp, "200 OK", "application/json; charset=utf-8", Encoding.UTF8.GetBytes("{\"ok\":true}"));
                }
                else if (ruta.StartsWith("/api/pagar"))
                {
                    // Permite acreditar un pago rápido a una tarjeta desde el Dashboard Web
                    string tarjeta = ExtraerParametroQuery(ruta, "tarjeta", "4509-1001");
                    string montoStr = ExtraerParametroQuery(ruta, "monto", "25000");
                    if (decimal.TryParse(montoStr, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal montoPago) && montoPago > 0)
                    {
                        ProcesarPagoDesdeGui(tarjeta, montoPago);
                    }
                    EnviarRespuestaHttp(clienteHttp, "200 OK", "application/json; charset=utf-8", Encoding.UTF8.GetBytes("{\"ok\":true}"));
                }
                else
                {
                    // Sirve el archivo dashboard.html
                    string rutaHtml = Path.Combine(AppContext.BaseDirectory, "dashboard.html");
                    if (!File.Exists(rutaHtml))
                    {
                        rutaHtml = Path.Combine(Directory.GetCurrentDirectory(), "dashboard.html");
                    }

                    if (File.Exists(rutaHtml))
                    {
                        byte[] bytesHtml = File.ReadAllBytes(rutaHtml);
                        EnviarRespuestaHttp(clienteHttp, "200 OK", "text/html; charset=utf-8", bytesHtml);
                    }
                    else
                    {
                        byte[] msg = Encoding.UTF8.GetBytes("<h1>Archivo dashboard.html no encontrado</h1>");
                        EnviarRespuestaHttp(clienteHttp, "404 Not Found", "text/html; charset=utf-8", msg);
                    }
                }
            }
            catch { }
            finally
            {
                try { clienteHttp.Shutdown(SocketShutdown.Both); } catch { }
                try { clienteHttp.Close(); } catch { }
            }
        }

        private static void EnviarRespuestaHttp(Socket socket, string estado, string contentType, byte[] cuerpo)
        {
            string cabeceras =
                $"HTTP/1.1 {estado}\r\n" +
                $"Content-Type: {contentType}\r\n" +
                $"Content-Length: {cuerpo.Length}\r\n" +
                "Access-Control-Allow-Origin: *\r\n" +
                "Cache-Control: no-cache\r\n" +
                "Connection: close\r\n\r\n";

            byte[] bytesCabecera = Encoding.UTF8.GetBytes(cabeceras);
            socket.Send(bytesCabecera);
            socket.Send(cuerpo);
        }

        private static string GenerarJsonEstadoActual()
        {
            lock (_bloqueoSeccionCritica)
            {
                var estado = new
                {
                    servidorPid = Environment.ProcessId,
                    horaServidor = DateTime.Now.ToString("HH:mm:ss"),
                    clientesConectados = _clientesConectados,
                    totalTransacciones = _totalTransacciones,
                    totalAprobadas = _totalAprobadas,
                    totalRechazadas = _totalRechazadas,
                    montoTotalCompras = _montoTotalComprasAprobadas,
                    montoTotalPagos = _montoTotalPagosRecibidos,
                    tarjetas = _tarjetas.Values,
                    rubros = _estadisticasRubros.Values,
                    terminales = _terminalesActivas.Values,
                    transacciones = _historialTransacciones
                };

                return JsonSerializer.Serialize(estado);
            }
        }

        private static void ProcesarPagoDesdeGui(string nroTarjeta, decimal monto)
        {
            lock (_bloqueoSeccionCritica)
            {
                if (!_tarjetas.TryGetValue(nroTarjeta, out CuentaTarjeta? cuenta)) return;

                _totalTransacciones++;
                cuenta.DeudaActual = Math.Max(0m, cuenta.DeudaActual - monto);
                _montoTotalPagosRecibidos += monto;
                _totalAprobadas++;

                _historialTransacciones.Insert(0, new RegistroTransaccion
                {
                    Numero = _totalTransacciones,
                    Hora = DateTime.Now.ToString("HH:mm:ss.fff"),
                    Terminal = "CAJA-WEB",
                    Rubro = "Pagos",
                    Tarjeta = cuenta.Numero,
                    Titular = cuenta.Titular,
                    Tipo = "PAGO",
                    Importe = monto,
                    Estado = "APROBADA",
                    Motivo = $"Pago acreditado vía Web ({cuenta.Titular})",
                    DisponibleRestante = cuenta.SaldoDisponible,
                    HiloId = Environment.CurrentManagedThreadId
                });

                if (_historialTransacciones.Count > 40)
                    _historialTransacciones.RemoveAt(_historialTransacciones.Count - 1);

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [PAGO DESDE GUI WEB] Tarjeta {cuenta.Numero} ({cuenta.Titular}) abonó ${monto:F2}. Nuevo disponible: ${cuenta.SaldoDisponible:F2}");
                Console.ResetColor();
            }
        }

        private static string ExtraerParametroQuery(string url, string clave, string valorDefecto)
        {
            int idx = url.IndexOf('?');
            if (idx < 0) return valorDefecto;
            string query = url.Substring(idx + 1);
            foreach (string par in query.Split('&'))
            {
                string[] kv = par.Split('=');
                if (kv.Length == 2 && kv[0].Equals(clave, StringComparison.OrdinalIgnoreCase))
                {
                    return kv[1];
                }
            }
            return valorDefecto;
        }

        private static string Recortar(string texto, int max)
        {
            if (string.IsNullOrEmpty(texto)) return "";
            return texto.Length <= max ? texto : texto.Substring(0, max);
        }

        private static void ImprimirBanner()
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("========================================================================================");
            Console.WriteLine("     SISTEMA AUTORIZADOR DE TARJETAS DE CRÉDITO Y TELEMETRÍA POS - SERVIDOR CENTRAL     ");
            Console.WriteLine("              UNAHUR - Sistemas Operativos 2C 2026 - Opción 5 (Sockets TCP)             ");
            Console.WriteLine("========================================================================================");
            Console.ResetColor();
        }
    }
}
